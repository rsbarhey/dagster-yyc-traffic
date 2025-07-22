import dagster as dg


@dg.asset
def traffic_incidents() -> None:
    print("test")