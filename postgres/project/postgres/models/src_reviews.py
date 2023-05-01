from ibis.expr.types.relations import Table

from visitran.materialization import Materialization
from visitran.templates.model import VisitranModel


class SourceReviews(VisitranModel):
    def __init__(self):
        super().__init__()
        self.materialization = Materialization.TABLE
        self.source_table_name = "reviews"
        self.source_schema_name = "src"
        self.destination_table_name = "modified_raw_reviews"
        self.destination_schema_name = "dev"
        self.database = "airbnb"

    def select(self) -> Table:
        t: Table = self.source_table_obj
        return t
