# frozen_string_literal: true

module DataDrain
  module Observability
    # Helper para medición de duración de operaciones.
    # @api private
    module Timing
      private

      def monotonic
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end

      def timed(step_name)
        t = monotonic
        result = yield
        @durations ||= {}
        @durations[step_name] = monotonic - t
        result
      end
    end
  end
end
