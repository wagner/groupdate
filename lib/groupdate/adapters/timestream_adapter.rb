module Groupdate
  module Adapters
    class AmazonTimestreamAdapter < BaseAdapter
      def group_clause
        raise Groupdate::Error, "Time zones not supported for Amazon Timestream" unless @time_zone.utc_offset.zero?

        day_start_column = "#{column} - INTERVAL '?' second"
        day_start_interval = day_start

        query =
          case period
          when :minute_of_hour
            ["EXTRACT(MINUTE FROM #{day_start_column})", day_start_interval]
          when :hour_of_day
            ["EXTRACT(HOUR FROM #{day_start_column})", day_start_interval]
          when :day_of_week
            ["EXTRACT(DOW FROM #{day_start_column})", day_start_interval]
          when :day_of_month
            ["EXTRACT(DAY FROM #{day_start_column})", day_start_interval]
          when :day_of_year
            ["EXTRACT(DOY FROM #{day_start_column})", day_start_interval]
          when :month_of_year
            ["EXTRACT(MONTH FROM #{day_start_column})", day_start_interval]
          when :week
            ["(DATE_TRUNC('day', #{day_start_column} - INTERVAL '1' day * ((? + EXTRACT(DOW FROM #{day_start_column})) % 7)) + INTERVAL '?' second)", day_start_interval, 13 - week_start, day_start_interval, day_start_interval]
          when :custom
            ["from_unixtime(FLOOR(to_unixtime(#{column}) / ?) * ?)", n_seconds, n_seconds]
          when :day, :month, :quarter, :year
            ["DATE_TRUNC(?, #{day_start_column})", period, day_start_interval]
          else
            # day start is always 0 for seconds, minute, hour
            ["DATE_TRUNC(?, #{day_start_column})", period, day_start_interval]
          end

        clean_group_clause(@relation.send(:sanitize_sql_array, query))
      end

      def clean_group_clause(clause)
        clause.gsub(/ (\-|\+) INTERVAL '0' second/, "")
      end
    end
  end
end
