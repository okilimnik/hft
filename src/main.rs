mod lightgbm;
mod alerts;

fn main() {
  //  lightgbm::train();
  //  lightgbm::predict();
  //let _result = dataset::collect().await;
  let _ = alerts::subscribe();
}