module Athenai
  describe SaveHistory do
    subject :handler do
      described_class.new(
        athena_client: athena_client,
        s3_client_factory: s3_client_factory,
        history_base_uri: history_base_uri,
        state_uri: state_uri,
        batch_size: 100,
        sleep_service: sleep_service,
        logger: logger,
      )
    end

    let :athena_client do
      Aws::Athena::Client.new(stub_responses: true).tap do |ac|
        allow(ac).to receive(:list_work_groups) do |parameters|
          ac.stub_data(:list_work_groups, work_groups: work_groups)
        end
        allow(ac).to receive(:list_query_executions) do |parameters|
          ids = Array(query_execution_ids[parameters[:work_group]])
          batches = [
            ids.take((ids.size * 0.6).to_i),
            ids.drop((ids.size * 0.6).to_i),
          ]
          if (token = parameters[:next_token])
            i = batches.index { |b| b.first == token }
            ac.stub_data(:list_query_executions, query_execution_ids: batches[i], next_token: batches[i + 1]&.first)
          else
            ac.stub_data(:list_query_executions, query_execution_ids: batches[0], next_token: batches[1].first)
          end
        end
        allow(ac).to receive(:batch_get_query_execution) do |parameters|
          query_executions = parameters[:query_execution_ids].map do |id|
            {
              query_execution_id: id,
              status: {
                submission_date_time: submission_date_time,
                completion_date_time: completion_date_time,
              },
            }
          end
          ac.stub_data(:batch_get_query_execution, query_executions: query_executions)
        end
      end
    end

    let :submission_date_time do
      Time.utc(2018, 12, 11, 10, 9, 8)
    end

    let :completion_date_time do
      Time.utc(2018, 12, 11, 10, 9, 8)
    end

    let :s3_client_factory do
      class_double(Aws::S3::Client, new: s3_client)
    end

    let :s3_client do
      Aws::S3::Client.new(stub_responses: true).tap do |sc|
        allow(sc).to receive(:get_bucket_location) do |parameters|
          if parameters[:bucket] == URI(history_base_uri).host
            sc.stub_data(:get_bucket_location, location_constraint: 'hi-story-3')
          elsif state_uri && parameters[:bucket] == URI(state_uri).host
            sc.stub_data(:get_bucket_location, location_constraint: 'st-ate-9')
          else
            sc.stub_data(:get_bucket_location, location_constraint: 'no-region-1')
          end
        end
        allow(sc).to receive(:put_object) do |parameters|
          if parameters[:key].start_with?('some/prefix/')
            zio = Zlib::GzipReader.new(StringIO.new(parameters[:body]))
            zio.each_line do |line|
              saved_executions << JSON.load(line)
            end
            zio.close
          end
          sc.stub_data(:put_object)
        end
        allow(sc).to receive(:get_object) do |parameters|
          if !state_uri.nil? && !state_contents.nil? && state_uri.end_with?(parameters[:key])
            sc.stub_data(:get_object, body: StringIO.new(JSON.dump(state_contents)))
          else
            raise Aws::S3::Errors::NoSuchKey.new(nil, nil)
          end
        end
      end
    end

    let :saved_executions do
      []
    end

    let :query_execution_ids do
      {
        'primary' => Array.new(11) { |i| format('q%02x', i) },
      }
    end

    let :work_groups do
      [
        {name: 'primary'},
      ]
    end

    let :history_base_uri do
      's3://athena-query-history/some/prefix/'
    end

    let :state_uri do
      nil
    end

    let :state_contents do
      nil
    end

    let :sleep_service do
      double(:sleep_service, sleep: nil)
    end

    let :logger do
      instance_double(Logger, debug: nil, info: nil, warn: nil)
    end

    describe '.handle' do
      before do
        allow(logger).to receive(:level=)
        allow(Aws::Athena::Client).to receive(:new).and_return(athena_client)
        allow(Aws::S3::Client).to receive(:new).and_return(s3_client)
        allow(Logger).to receive(:new).and_return(logger)
      end

      before do
        ENV['HISTORY_BASE_URI'] = 's3://history/base/uri'
        ENV['STATE_URI'] = 's3://state/uri.json'
      end

      after do
        ENV.delete('HISTORY_BASE_URI')
        ENV.delete('STATE_URI')
      end

      it 'initializes the handler with its dependencies and calls #save_history' do
        described_class.handler(event: nil, context: nil)
        expect(athena_client).to have_received(:list_query_executions).at_least(:once)
      end

      it 'returns whatever #save_history returns' do
        expect(described_class.handler(event: nil, context: nil)).to eq('primary' => 'q00')
      end

      it 'picks up the history URI from the environment' do
        described_class.handler(event: nil, context: nil)
        expect(s3_client).to have_received(:put_object).with(hash_including(bucket: 'history', key: including('base/uri/')))
      end

      it 'picks up the state URI from the environment' do
        described_class.handler(event: nil, context: nil)
        expect(s3_client).to have_received(:put_object).with(hash_including(bucket: 'state', key: including('uri.json')))
      end
    end

    describe '#save_history' do
      it 'looks up the region of the history URI\'s bucket and creates an S3 client for that region' do
        handler.save_history
        expect(s3_client_factory).to have_received(:new).with(region: 'hi-story-3')
      end

      it 'logs the region it detects for the bucket' do
        handler.save_history
        expect(logger).to have_received(:debug).with('Detected region of bucket athena-query-history as hi-story-3')
      end

      it 'lists the query executions' do
        handler.save_history
        expect(athena_client).to have_received(:list_query_executions).at_least(:once)
      end

      it 'looks up the query executions' do
        handler.save_history
        expect(athena_client).to have_received(:batch_get_query_execution).with(query_execution_ids: query_execution_ids['primary'])
      end

      it 'logs when it loads query execution metadata' do
        handler.save_history
        expect(logger).to have_received(:debug).with('Loading query execution metadata for 11 query executions')
      end

      it 'logs the submission date time of the last query execution of the batch' do
        handler.save_history
        expect(logger).to have_received(:debug).with('Last submission time of the batch was 2018-12-11 10:09:08 UTC')
      end

      it 'stores the query execution metadata in a key that contains the region, the month it was submitted, work group, and ID of the first processed query execution ID' do
        handler.save_history
        expect(s3_client).to have_received(:put_object).with(hash_including(key: 'some/prefix/region=us-stubbed-1/month=2018-12-01/work_group=primary/q00.json.gz'))
      end

      it 'stores the query execution metadata on S3 in the specified bucket and prefix' do
        handler.save_history
        expect(s3_client).to have_received(:put_object).with(hash_including(bucket: 'athena-query-history', key: start_with('some/prefix/')))
      end

      it 'logs when it stores query execution metadata' do
        handler.save_history
        expect(logger).to have_received(:debug).with('Saving execution metadata for 11 queries to s3://athena-query-history/some/prefix/region=us-stubbed-1/month=2018-12-01/work_group=primary/q00.json.gz')
        expect(logger).to have_received(:info).with('Saved execution metadata for 11 queries')
      end

      it 'stores the query execution metadata as JSON streams' do
        handler.save_history
        expect(saved_executions).to contain_exactly(
          hash_including('query_execution_id' => 'q00'),
          hash_including('query_execution_id' => 'q01'),
          hash_including('query_execution_id' => 'q02'),
          hash_including('query_execution_id' => 'q03'),
          hash_including('query_execution_id' => 'q04'),
          hash_including('query_execution_id' => 'q05'),
          hash_including('query_execution_id' => 'q06'),
          hash_including('query_execution_id' => 'q07'),
          hash_including('query_execution_id' => 'q08'),
          hash_including('query_execution_id' => 'q09'),
          hash_including('query_execution_id' => 'q0a'),
        )
      end

      it 'formats the timestamps in the query execution metadata in the UTC time zone and in a format compatible with Hive' do
        handler.save_history
        expect(saved_executions.first.dig('status', 'submission_date_time')).to eq('2018-12-11 10:09:08.000')
        expect(saved_executions.first.dig('status', 'completion_date_time')).to eq('2018-12-11 10:09:08.000')
      end

      it 'adds the region to the query execution metadata' do
        handler.save_history
        expect(saved_executions.first).to include('region' => 'us-stubbed-1')
      end

      it 'logs when it is done' do
        handler.save_history
        expect(logger).to have_received(:info).with('Done')
      end

      it 'returns the first processed query execution ID for each work group' do
        expect(handler.save_history).to eq('primary' => 'q00')
      end

      context 'when there are multiple work groups' do
        let :query_execution_ids do
          {
            'primary' => Array.new(11) { |i| format('q%02x', i) },
            'secondary' => Array.new(11) { |i| format('q%02x', i + 32) },
          }
        end

        let :work_groups do
          [
            {name: 'primary'},
            {name: 'secondary'},
          ]
        end

        it 'lists the work groups' do
          handler.save_history
          expect(athena_client).to have_received(:list_work_groups)
        end

        it 'loads the history from each work group' do
          handler.save_history
          expect(athena_client).to have_received(:list_query_executions).with(hash_including(work_group: 'primary')).at_least(:once)
          expect(athena_client).to have_received(:list_query_executions).with(hash_including(work_group: 'secondary')).at_least(:once)
        end

        it 'stores the history for each work group' do
          handler.save_history
          expect(saved_executions).to include(
            hash_including('query_execution_id' => 'q00'),
            hash_including('query_execution_id' => 'q01'),
            hash_including('query_execution_id' => 'q20'),
            hash_including('query_execution_id' => 'q21'),
          )
          expect(s3_client).to have_received(:put_object).with(hash_including(key: 'some/prefix/region=us-stubbed-1/month=2018-12-01/work_group=primary/q00.json.gz'))
          expect(s3_client).to have_received(:put_object).with(hash_including(key: 'some/prefix/region=us-stubbed-1/month=2018-12-01/work_group=secondary/q20.json.gz'))
        end

        it 'logs when it starts processing a work group' do
          handler.save_history
          expect(logger).to have_received(:debug).with('Loading query execution history for work group "primary"')
          expect(logger).to have_received(:debug).with('Loading query execution history for work group "secondary"')
        end
      end

      context 'when no history URI has been specified' do
        let :history_base_uri do
          nil
        end

        it 'raises an error' do
          expect { handler.save_history }.to raise_error(ArgumentError, 'No history base URI specified')
        end
      end

      context 'when there are more than 50 query executions' do
        let :query_execution_ids do
          super().merge(
            'primary' => Array.new(121) { |i| format('q%02x', i) },
          )
        end

        it 'looks up the query executions 50 at a time' do
          handler.save_history
          expect(athena_client).to have_received(:batch_get_query_execution).with(query_execution_ids: query_execution_ids['primary'].take(50))
          expect(athena_client).to have_received(:batch_get_query_execution).with(query_execution_ids: query_execution_ids['primary'].drop(50).take(50))
          expect(athena_client).to have_received(:batch_get_query_execution).with(query_execution_ids: query_execution_ids['primary'].drop(100))
        end

        context 'and the number is evenly divisible by 50' do
          let :query_execution_ids do
            super().merge(
              'primary' => Array.new(100) { |i| format('q%02x', i) },
            )
          end

          it 'does not make an empty lookup call' do
            handler.save_history
            expect(athena_client).to_not have_received(:batch_get_query_execution).with(query_execution_ids: [])
          end
        end
      end

      context 'when there are more query executions than the batch size' do
        let :query_execution_ids do
          super().merge(
            'primary' => Array.new(211) { |i| format('q%02x', i) },
          )
        end

        it 'stores the query executions one batch at a time' do
          handler.save_history
          expect(s3_client).to have_received(:put_object).with(hash_including(key: 'some/prefix/region=us-stubbed-1/month=2018-12-01/work_group=primary/q00.json.gz'))
          expect(s3_client).to have_received(:put_object).with(hash_including(key: 'some/prefix/region=us-stubbed-1/month=2018-12-01/work_group=primary/q64.json.gz'))
          expect(s3_client).to have_received(:put_object).with(hash_including(key: 'some/prefix/region=us-stubbed-1/month=2018-12-01/work_group=primary/qc8.json.gz'))
          expect(saved_executions.size).to eq(211)
        end
      end

      context 'when there are no query executions' do
        let :query_execution_ids do
          {}
        end

        it 'does not store any data on S3' do
          handler.save_history
          expect(s3_client).to_not have_received(:put_object)
        end

        it 'returns an empty hash' do
          expect(handler.save_history).to be_empty
        end
      end

      context 'when given a state key' do
        let :state_uri do
          's3://state/some/other/prefix/key.json'
        end

        let :state_contents do
          {
            'work_groups' => {
              'primary' => {'last_query_execution_id' => 'q03'},
              'secondary' => {'last_query_execution_id' => 'q22'},
            },
          }
        end

        let :work_groups do
          [
            {name: 'primary'},
            {name: 'secondary'},
          ]
        end

        let :query_execution_ids do
          {
            'primary' => Array.new(11) { |i| format('q%02x', i) },
            'secondary' => Array.new(11) { |i| format('q%02x', i + 32) },
          }
        end

        it 'looks up the region for the history URIs and creates an S3 client for that region' do
          handler.save_history
          expect(s3_client_factory).to have_received(:new).with(region: 'st-ate-9')
        end

        it 'attempts to load the last query execution ID from the given key in the history bucket' do
          handler.save_history
          expect(saved_executions.size).to eq(5)
        end

        it 'loads query execution metadata until it finds the last query execution ID given by the state' do
          handler.save_history
          expect(saved_executions).to contain_exactly(
            hash_including('query_execution_id' => 'q00'),
            hash_including('query_execution_id' => 'q01'),
            hash_including('query_execution_id' => 'q02'),
            hash_including('query_execution_id' => 'q20'),
            hash_including('query_execution_id' => 'q21'),
          )
        end

        it 'logs the last query execution ID of each work group' do
          handler.save_history
          expect(logger).to have_received(:info).with('Loaded last query execution ID for work group "primary": "q03"')
          expect(logger).to have_received(:info).with('Loaded last query execution ID for work group "secondary": "q22"')
        end

        it 'logs when it finds the last query execution ID' do
          handler.save_history
          expect(logger).to have_received(:info).with('Found the last previously processed query execution ID for work group "primary"')
        end

        it 'stores the first query execution IDs per work group in an object at the specified URI' do
          last_saved_state = nil
          allow(s3_client).to receive(:put_object) do |parameters|
            if parameters[:bucket] == 'state' && parameters[:key] == 'some/other/prefix/key.json'
              last_saved_state = JSON.load(parameters[:body])
            end
          end
          handler.save_history
          expect(last_saved_state['work_groups']).to include(
            'primary' => {'last_query_execution_id' => 'q00'},
            'secondary' => {'last_query_execution_id' => 'q20'},
          )
        end

        it 'logs when it loads the state' do
          handler.save_history
          expect(logger).to have_received(:debug).with('Loading state from s3://state/some/other/prefix/key.json')
          expect(logger).to have_received(:info).with('Loaded last query execution ID for work group "primary": "q03"')
        end

        it 'logs when it stores the state' do
          handler.save_history
          expect(logger).to have_received(:debug).with('Saving state to s3://state/some/other/prefix/key.json').at_least(:once)
          expect(logger).to have_received(:info).with('Saved first processed query execution ID for work group "primary": "q00"')
        end

        context 'and the state contents are from before work groups were released' do
          let :state_contents do
            {'last_query_execution_id' => 'q03'}
          end

          it 'interprets the state as if it contains the state for only the primary work group' do
            handler.save_history
            expect(saved_executions).to include(
              hash_including('query_execution_id' => 'q00'),
              hash_including('query_execution_id' => 'q01'),
              hash_including('query_execution_id' => 'q02'),
            )
            expect(saved_executions).to_not include(
              hash_including('query_execution_id' => 'q03'),
            )
          end

          it 'saves the new state in the new format' do
            handler.save_history
            expected_body = JSON.dump('work_groups' => {
              'primary' => {'last_query_execution_id' => 'q00'},
            })
            expect(s3_client).to have_received(:put_object).with(bucket: 'state', key: 'some/other/prefix/key.json', body: expected_body)
          end
        end

        context 'and the state key does not exist' do
          let :state_contents do
            nil
          end

          it 'acts as if the last query execution ID was nil' do
            handler.save_history
            expect(saved_executions.size).to eq(22)
          end

          it 'still stores the first query execution ID' do
            handler.save_history
            body = JSON.dump('work_groups' => {'primary' => {'last_query_execution_id' => 'q00'}})
            expect(s3_client).to have_received(:put_object).with(bucket: 'state', key: 'some/other/prefix/key.json', body: body)
          end

          it 'logs that it did not find any state' do
            handler.save_history
            expect(logger).to have_received(:warn).with('No state found at s3://state/some/other/prefix/key.json')
          end
        end

        context 'and the state contains other information than the last query execution ID' do
          let :state_contents do
            {
              'work_groups' => {
                'primary' => {
                  'last_query_execution_id' => 'q03',
                }
              },
              'something' => 'else',
            }
          end

          it 'retains that data when it saves the state back' do
            handler.save_history
            body = JSON.dump('work_groups' => {'primary' => {'last_query_execution_id' => 'q00'}}, 'something' => 'else')
            expect(s3_client).to have_received(:put_object).with(bucket: 'state', key: 'some/other/prefix/key.json', body: body)
          end
        end

        context 'and there are more query executions than the batch size' do
          let :state_contents do
            {
              'work_groups' => {
                'primary' => {
                  'last_query_execution_id' => 'qff',
                }
              },
            }
          end

          let :query_execution_ids do
            super().merge(
              'primary' => Array.new(211) { |i| format('q%02x', i) },
            )
          end

          it 'stores the state once per work group processed' do
            handler.save_history
            expect(s3_client).to have_received(:put_object).with(hash_including(key: 'some/other/prefix/key.json')).exactly(2).times
          end
        end
      end

      context 'when a query does not have a completion time' do
        let :completion_date_time do
          nil
        end

        it 'doesn\'t attempt to format it' do
          handler.save_history
          expect(saved_executions.first.dig('status', 'submission_date_time')).to eq('2018-12-11 10:09:08.000')
          expect(saved_executions.first.dig('status', 'completion_date_time')).to be_nil
        end
      end

      context 'when an API call raises a throttling error' do
        let :athena_client do
          super().tap do |ac|
            list_groups_attempts = 0
            allow(ac).to receive(:list_work_groups) do
              list_groups_attempts += 1
              if list_groups_attempts < 5
                raise Aws::Athena::Errors::ThrottlingException.new(nil, nil)
              else
                ac.stub_data(:list_work_groups, work_groups: work_groups)
              end
            end
            list_executions_attempts = 0
            allow(ac).to receive(:list_query_executions) do
              list_executions_attempts += 1
              if list_executions_attempts < 8
                raise Aws::Athena::Errors::ThrottlingException.new(nil, nil)
              else
                ac.stub_data(:list_query_executions, query_execution_ids: query_execution_ids['primary'])
              end
            end
            get_attempts = 0
            allow(ac).to receive(:batch_get_query_execution) do
              get_attempts += 1
              if get_attempts < 3
                raise Aws::Athena::Errors::ThrottlingException.new(nil, nil)
              else
                ac.stub_data(:batch_get_query_execution)
              end
            end
          end
        end

        it 'tries again' do
          handler.save_history
          expect(athena_client).to have_received(:list_work_groups).exactly(5).times
          expect(athena_client).to have_received(:list_query_executions).exactly(8).times
          expect(athena_client).to have_received(:batch_get_query_execution).exactly(3).times
        end

        it 'backs off on each attempt, up to a max duration' do
          sleep_durations = []
          allow(sleep_service).to receive(:sleep) do |n|
            sleep_durations << n
          end
          handler.save_history
          expect(sleep_durations).to eq([1, 2, 4, 8, 1, 2, 4, 8, 16, 16, 16, 1, 2])
        end
      end

      context 'when a bucket is located in us-east-1' do
        let :s3_client do
          super().tap do |sc|
            allow(sc).to receive(:get_bucket_location) do |parameters|
              sc.stub_data(:get_bucket_location, location_constraint: '')
            end
          end
        end

        it 'converts the empty location constrainet returned by GetBucketLocation to "us-east-1"' do
          handler.save_history
          expect(s3_client_factory).to have_received(:new).with(region: 'us-east-1')
        end
      end
    end
  end
end
