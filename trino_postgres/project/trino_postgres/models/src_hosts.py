from ibis.expr.types.relations import Table

from visitran.materialization import Materialization
from visitran.templates.model import VisitranModel


class SourceHosts(VisitranModel):
    def __init__(self) -> None:
        super().__init__()
        self.materialization = Materialization.TABLE
        self.source_table_name = "hosts"
        self.source_schema_name = "raw"
        self.destination_table_name = "modified_raw_hosts"
        self.destination_schema_name = "dev"
        self.database = "airbnb"

    def select(self) -> Table:
        raw_hosts: Table = self.source_table_obj
        rh = raw_hosts
        # field names won't be available in intellisense, but will be in breakpoints
        return raw_hosts[
            "created_at",
            rh.id.name("host_id"),
            "is_superhost",
            rh.name.name("host_name"),
            "updated_at",
        ]
