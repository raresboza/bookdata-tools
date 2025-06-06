use crate::{ids::codes::NS_GR_WORK, prelude::*};
use polars::prelude::*;

pub fn link_work_genders() -> Result<()> {
    require_working_dir("goodreads")?;

    let gender = LazyFrame::scan_parquet("../book-links/cluster-genders.parquet", default())?;
    let books = LazyFrame::scan_parquet("gr-book-link.parquet", default())?;

    let merged = gender.join(
        books,
        &[col("cluster")],
        &[col("cluster")],
        JoinType::Inner.into(),
    );
    let dedup = merged.unique(None, UniqueKeepStrategy::First);
    let dedup = dedup.select([
        col("*"),
        coalesce(&[
            col("work_id") + lit(NS_GR_WORK.base()),
            col("book_id") + lit(NS_GR_WORK.base()),
        ])
        .alias("gr_item"),
    ]);

    info!("computing results");
    let results = dedup.collect()?;

    info!("saving {} work-gender records", results.height());
    save_df_parquet(results, "gr-work-gender.parquet")?;

    Ok(())
}
