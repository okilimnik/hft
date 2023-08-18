use average::{Variance, Quantile, Estimate, concatenate};

concatenate!(Estimator,
    [Variance, variance, mean, sample_variance],
    [Quantile, quantile, quantile]);

pub fn calc() {
    let s: Estimator = (1..6).map(f64::from).collect();
    println!("{:?}", s.sample_variance());
    println!("{:?}", s.mean());  
    println!("{:?}", s.quantile());  
}