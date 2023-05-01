from ibis.expr.types.relations import Table

from visitran.materialization import Materialization
from visitran.templates.model import VisitranModel


class SourceListings(VisitranModel):
    def __init__(self):
        super().__init__()
        self.materialization = Materialization.TABLE
        self.source_table_name = "listings"
        self.source_schema_name = "src"
        self.destination_table_name = "modified_raw_listings"
        self.destination_schema_name = "dev"
        self.database = "airbnb"

    def select(self) -> Table:
        raw_listings: Table = self.source_table_obj
        rl = raw_listings
        return raw_listings[
            rl.id.name("listing_id"),
            rl.name.name("listing_name"),
            "listing_url",
            "room_type",
            "minimum_nights",
            "host_id",
            rl.price.name("price_str"),
            "created_at",
            "updated_at",
        ]
