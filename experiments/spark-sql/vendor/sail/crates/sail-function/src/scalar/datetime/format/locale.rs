// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LocaleData {
    pub(crate) months_short: [&'static str; 12],
    pub(crate) months_full: [&'static str; 12],
    pub(crate) weekdays_short: [&'static str; 7],
    pub(crate) weekdays_full: [&'static str; 7],
    pub(crate) quarters_short: [&'static str; 4],
    pub(crate) quarters_full: [&'static str; 4],
    pub(crate) eras_short: [&'static str; 2],
    pub(crate) eras_full: [&'static str; 2],
    pub(crate) am_pm: [&'static str; 2],
}

pub(crate) const EN_US: LocaleData = LocaleData {
    months_short: [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ],
    months_full: [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ],
    weekdays_short: ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
    weekdays_full: [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ],
    quarters_short: ["Q1", "Q2", "Q3", "Q4"],
    quarters_full: ["1st quarter", "2nd quarter", "3rd quarter", "4th quarter"],
    eras_short: ["BC", "AD"],
    eras_full: ["Before Christ", "Anno Domini"],
    am_pm: ["AM", "PM"],
};
