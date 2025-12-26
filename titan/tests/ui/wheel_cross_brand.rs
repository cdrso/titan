use generativity::{Guard, make_guard};
use std::num::{NonZeroU64, NonZeroUsize};
use titan::runtime::timing::{Duration, Micros, NonZeroDuration, Wheel};

type Wheel8<'id> = Wheel<'id, u32, Micros, 8>;

fn with_guard<F>(f: F)
where
    F: for<'id> FnOnce(Guard<'id>),
{
    make_guard!(guard);
    f(guard);
}

fn main() {
    with_guard(|guard_a| {
        let wheel_a: Wheel8<'_> = Wheel::new(
            guard_a,
            NonZeroDuration::new(NonZeroU64::new(1).unwrap()),
            NonZeroUsize::new(4).unwrap(),
        );
        let now_a = wheel_a.now();
        with_guard(|guard_b| {
            let mut wheel_b: Wheel8<'_> = Wheel::new(
                guard_b,
                NonZeroDuration::new(NonZeroU64::new(1).unwrap()),
                NonZeroUsize::new(4).unwrap(),
            );
            let _ = wheel_b.schedule_after(&now_a, Duration::<Micros>::new(1), 7);
        });
    });
}
