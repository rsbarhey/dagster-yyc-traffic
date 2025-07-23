import dagster as dg

start_date = "2023-01-01"
end_date = "2023-01-08"

daily_partition = dg.DailyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)