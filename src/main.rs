//mod lightgbm;
mod alerts;
mod stats;
//mod telegram;

fn main() {
  //  lightgbm::train();
  //  lightgbm::predict();
  //let _result = dataset::collect().await;

  //let _ = alerts::subscribe();
  stats::calc();
  //telegram::start_bot().await;
}