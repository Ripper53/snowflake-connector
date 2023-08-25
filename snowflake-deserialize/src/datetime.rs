use anyhow::anyhow;

use crate::DeserializeFromStr;

impl DeserializeFromStr for time::OffsetDateTime {
    fn deserialize_from_str(s: Option<&str>) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let s = s.ok_or_else(|| anyhow!("Unexpected null when parsing DateTime"))?;
        let f = s
            .parse::<f64>()
            .map_err(|err| anyhow!("expected DateTime as float. But got `{s}`: {err}"))?;

        Ok(f_to_date(f))
    }
}

fn f_to_date(f: f64) -> time::OffsetDateTime {
    use time::Duration;

    let dt = if f < 1. {
        time::OffsetDateTime::UNIX_EPOCH
    } else if f < 60. {
        time::macros::datetime!(1899-12-31 00:00:00).assume_utc()
    } else {
        time::macros::datetime!(1899-12-30 00:00:00).assume_utc()
    };

    let days = f.floor();
    let part_day = f - days;
    let hours = (part_day * 24.0).floor();
    let part_day = part_day * 24f64 - hours;
    let minutes = (part_day * 60f64).floor();
    let part_day = part_day * 60f64 - minutes;
    let seconds = (part_day * 60f64).round();

    dt + Duration::days(days as i64)
        + Duration::hours(hours as i64)
        + Duration::minutes(minutes as i64)
        + Duration::seconds(seconds as i64)
}
