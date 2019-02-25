require 'logger'
require 'time'
require 'zlib'
require 'stringio'
require 'uri'
require 'aws-sdk-athena'
require 'aws-sdk-s3'

module Athenai
  class SaveHistory
    MAX_GET_QUERY_EXECUTION_BATCH_SIZE = 50

    def initialize(athena_client:, s3_client_factory:, history_base_uri:, batch_size: 10_000, state_uri: nil, sleep_service: Kernel, logger: nil)
      unless history_base_uri
        raise ArgumentError, 'No history base URI specified'
      end
      @athena_client = athena_client
      @s3_client_factory = s3_client_factory
      @history_base_uri = URI(history_base_uri)
      @state_uri = state_uri && URI(state_uri)
      @batch_size = batch_size
      @sleep_service = sleep_service
      @logger = logger
      @last_query_execution_id = nil
      @state_saved = false
    end

    def self.handler(event:, context:)
      athena_client = Aws::Athena::Client.new
      s3_client_factory = Aws::S3::Client
      logger = Logger.new($stderr)
      logger.level = Logger::DEBUG
      handler = new(
        athena_client: athena_client,
        s3_client_factory: s3_client_factory,
        history_base_uri: ENV['HISTORY_BASE_URI'],
        state_uri: ENV['STATE_URI'],
        logger: logger,
      )
      handler.save_history
    end

    def save_history
      load_state
      first_query_execution_ids = {}
      each_work_group do |work_group|
        @logger.debug(format('Loading query execution history for work group "%s"', work_group.name))
        first_query_execution_ids[work_group.name] = store_work_group_history(work_group)
      end
      @logger.info('Done')
      first_query_execution_ids.reject { |_, v| v.nil? }
    end

    private def store_work_group_history(work_group)
      first_id = nil
      ids = []
      query_executions = []
      catch :done do
        each_query_execution_id(work_group) do |query_execution_id|
          if query_execution_id == @last_query_execution_id
            @logger.info(format('Found the last previously processed query execution ID for work group "%s"', work_group.name))
            throw :done
          else
            first_id ||= query_execution_id
            ids << query_execution_id
            if ids.size == MAX_GET_QUERY_EXECUTION_BATCH_SIZE
              query_executions.concat(load_query_execution_metadata(ids))
              if query_executions.size >= @batch_size
                save_query_execution_metadata(work_group, query_executions)
                save_state(work_group.name => first_id)
                query_executions = []
              end
              ids = []
            end
          end
        end
      end
      unless ids.empty?
        query_executions.concat(load_query_execution_metadata(ids))
      end
      unless query_executions.empty?
        save_query_execution_metadata(work_group, query_executions)
        save_state(work_group.name => first_id)
      end
      first_id
    end

    private def create_s3_client(bucket)
      region = @s3_client_factory.new.get_bucket_location(bucket: bucket).location_constraint
      region = 'us-east-1' if region.empty?
      @logger.debug(format('Detected region of bucket %s as %s', bucket, region))
      @s3_client_factory.new(region: region)
    end

    private def state_s3_client
      @state_s3_client ||= create_s3_client(@state_uri.host)
    end

    private def history_s3_client
      @history_s3_client ||= create_s3_client(@history_base_uri.host)
    end

    private def load_state
      if @state_uri
        begin
          @logger.debug(format('Loading state from %s', @state_uri))
          response = state_s3_client.get_object(bucket: @state_uri.host, key: @state_uri.path[1..-1])
          @state = JSON.load(response.body)
          @last_query_execution_id = @state.dig('work_groups', 'primary', 'last_query_execution_id') || @state.delete('last_query_execution_id')
          @logger.info(format('Loaded last query execution ID: "%s"', @last_query_execution_id))
        rescue Aws::S3::Errors::NoSuchKey
          @state = {}
          @logger.warn(format('No state found at %s', @state_uri))
        end
      end
    end

    private def each_work_group(&block)
      next_token = nil
      loop do
        response = retry_throttling do
          @athena_client.list_work_groups
        end
        response.work_groups.each(&block)
        if (t = response.next_token)
          next_token = t
        else
          break
        end
      end
    end

    private def each_query_execution_id(work_group, &block)
      next_token = nil
      loop do
        response = retry_throttling do
          @athena_client.list_query_executions(work_group: work_group.name, next_token: next_token)
        end
        response.query_execution_ids.each(&block)
        if (t = response.next_token)
          next_token = t
        else
          break
        end
      end
    end

    private def retry_throttling
      attempts = 0
      begin
        attempts += 1
        yield
      rescue Aws::Athena::Errors::ThrottlingException
        @sleep_service.sleep([2**(attempts - 1), 16].min)
        retry
      end
    end

    private def load_query_execution_metadata(query_execution_ids)
      @logger.debug(format('Loading query execution metadata for %d query executions', query_execution_ids.size))
      response = retry_throttling do
        @athena_client.batch_get_query_execution(query_execution_ids: query_execution_ids)
      end
      if (last = response.query_executions.last)
        time = last.status.submission_date_time.dup.utc
        @logger.debug(time.strftime('Last submission time of the batch was %F %T %Z'))
      end
      response.query_executions
    end

    private def create_metadata_log_contents(query_executions)
      region = @athena_client.config.region
      zio = Zlib::GzipWriter.new(StringIO.new)
      query_executions.each do |query_execution|
        h = query_execution.to_h
        s = h.dig(:status, :submission_date_time).dup.utc
        c = h.dig(:status, :completion_date_time).dup&.utc
        h = h.merge(
          region: region,
          status: h[:status].merge(
            submission_date_time: s.strftime('%F %T.%L'),
            completion_date_time: c&.strftime('%F %T.%L'),
          ),
        )
        zio.puts(JSON.dump(h))
      end
      zio.close.string
    end

    private def create_metadata_log_key(prefix, work_group, first_query_execution)
      key = prefix.dup
      key << '/' unless key.end_with?('/')
      key << 'region='
      key << @athena_client.config.region
      key << '/month='
      key << first_query_execution.status.submission_date_time.strftime('%Y-%m-01')
      key << '/work_group='
      key << work_group.name
      key << '/'
      key << first_query_execution.query_execution_id
      key << '.json.gz'
      key
    end

    private def save_state(first_query_execution_id_by_workgroup)
      if @state_uri && !@state_saved
        @logger.debug(format('Saving state to %s', @state_uri))
        @state['work_groups'] ||= {}
        first_query_execution_id_by_workgroup.each do |work_group, query_execution_id|
          @state['work_groups'][work_group] ||= {}
          @state['work_groups'][work_group]['last_query_execution_id'] = query_execution_id
        end
        body = JSON.dump(@state)
        state_s3_client.put_object(bucket: @state_uri.host, key: @state_uri.path[1..-1], body: body)
        first_query_execution_id_by_workgroup.each do |work_group, query_execution_id|
          @logger.info(format('Saved first processed query execution ID for work group "%s": "%s"', work_group, query_execution_id))
        end
        @state_saved = true
      end
    end

    private def save_query_execution_metadata(work_group, query_executions)
      first_query_execution = query_executions.first
      body = create_metadata_log_contents(query_executions)
      key = create_metadata_log_key(@history_base_uri.path[1..-1], work_group, first_query_execution)
      @logger.debug(format('Saving execution metadata for %d queries to s3://%s/%s', query_executions.size, @history_base_uri.host, key))
      history_s3_client.put_object(bucket: @history_base_uri.host, key: key, body: body)
      @logger.info(format('Saved execution metadata for %d queries', query_executions.size))
      first_query_execution
    end
  end
end
