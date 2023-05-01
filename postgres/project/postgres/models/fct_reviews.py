from ibis.expr.types.relations import Table
from postgres.models.src_reviews import SourceReviews

from visitran.materialization import Materialization


class FctReviews(SourceReviews):
    def __init__(self):
        super().__init__()
        self.materialization = Materialization.INCREMENTAL
        self.source_table_name = "modified_raw_reviews"
        self.source_schema_name = "dev"

        self.destination_table_name = "fct_reviews"
        self.destination_schema_name = "dev"
        self.database = "airbnb"

    def select(self) -> Table:
        source_table: Table = SourceReviews().destination_table_obj
        return source_table

    # select statement with constraints
    def select_if_incremental(self) -> Table:
        source_table: Table = SourceReviews().destination_table_obj
        # source_table: Table = self.source_table_obj # also will work
        # source_table: Table = self.select() # also will work

        this_table: Table = self.destination_table_obj
        new_table = source_table.filter(
            source_table.date > this_table["date"].max().name("latest_date")
        )
        return new_table


# INSERT INTO raw.raw_reviews(
# 	listing_id, date, reviewer_name, comments, sentiment)
# 	VALUES (8888, now(), 'Arun K B', 'AC was not working', 'negative');

# DELETE FROM "raw".raw_reviews
# WHERE listing_id=8888;
