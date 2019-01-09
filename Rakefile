require 'rspec/core/rake_task'

namespace :docker do
  task :build do
    sh 'docker build . -t burtcorp/athenai'
  end

  task :push => :build do
    sh 'docker push burtcorp/athenai'
  end
end

desc 'Run specs'
RSpec::Core::RakeTask.new(:spec) do |task|
  task.rspec_opts = %w[--format doc]
end

task default: :spec
