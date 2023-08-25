use anyhow::anyhow;

use crate::DeserializeFromStr;

/// Impl for Date.
///
/// We need to fail if the Float contains decimals, this is a DateTime, not a Date.
///
impl DeserializeFromStr for time::Date {
    fn deserialize_from_str(s: Option<&str>) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let s = s.ok_or_else(|| anyhow!("Unexpected null when parsing DateTime"))?;
        let f = s
            .parse::<f32>()
            .map_err(|err| anyhow!("expected DateTime as float. But got `{s}`: {err}"))?;

        let unix_ts = f as i64;

        time::OffsetDateTime::from_unix_timestamp(f as i64)
            .map_err(|err| anyhow!("Invalid Unix TS `{unix_ts}` (as float: `{f}`): {err}"))
            .map(|dt| dt.date())
    }
}

//fn f_to_date(f: f64) -> time::OffsetDateTime {
//    use time::Duration;
//
//    let dt = if f < 1. {
//        time::OffsetDateTime::UNIX_EPOCH
//    } else if f < 60. {
//        time::macros::datetime!(1899-12-31 00:00:00).assume_utc()
//    } else {
//        time::macros::datetime!(1899-12-30 00:00:00).assume_utc()
//    };
//
//    let days = f.floor();
//    let part_day = f - days;
//    let hours = (part_day * 24.0).floor();
//    let part_day = part_day * 24f64 - hours;
//    let minutes = (part_day * 60f64).floor();
//    let part_day = part_day * 60f64 - minutes;
//    let seconds = (part_day * 60f64).round();
//
//    eprintln!("days: {days}");
//    eprintln!("hours: {hours}");
//    eprintln!("minutes: {minutes}");
//    eprintln!("seconds: {seconds}");
//
//    dt + Duration::days(days as i64)
//        + Duration::hours(hours as i64)
//        + Duration::minutes(minutes as i64)
//        + Duration::seconds(seconds as i64)
//}
