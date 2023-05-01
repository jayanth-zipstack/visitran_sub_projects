from ibis.expr.types.relations import Table
from postgres.models.src_hosts import SourceHosts

from visitran.materialization import Materialization


class DimHostsCleansed(SourceHosts):
    def __init__(self):
        super().__init__()
        self.materialization = Materialization.TABLE
        self.destination_table_name = "dim_hosts_cleansed"
        self.destination_schema_name = "dev"
        self.database = "airbnb"

    def select(self) -> Table:
        sh = SourceHosts().destination_table_obj
        return sh[
            "host_id",
            sh.host_name.coalesce(sh.host_name, "Anonymous").name("host_name"),
            "is_superhost",
            "created_at",
            "updated_at",
        ]
