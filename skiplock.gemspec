lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

require "skiplock/version"

Gem::Specification.new do |spec|
  spec.name          = "skiplock"
  spec.version       = Skiplock::VERSION
  spec.authors       = ["Tin Vo"]
  spec.email         = ["vtt999@gmail.com"]

  spec.summary       = %q{ActiveJob Queue Adapter for PostgreSQL}
  spec.description   = %q{High performance ActiveJob Queue Adapter for PostgreSQL that provides maximum reliability and ACID compliance}
  spec.homepage      = "https://github.com/vtt/skiplock"
  spec.license       = "MIT"

  spec.files         = Dir['lib/**/*', 'README.md', 'LICENSE.txt']
  spec.executables   = %w[ skiplock ]
  spec.require_paths = ["lib"]
  spec.required_ruby_version = ">= 2.5.0"
  spec.add_dependency "activejob", ">= 5.2.0"
  spec.add_dependency "activerecord", ">= 5.2.0"
  spec.add_dependency "concurrent-ruby", ">= 1.0.2"
  spec.add_dependency "parse-cron", "~> 0.1"
end
