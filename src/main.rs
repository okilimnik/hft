//mod lightgbm;
mod alerts;
//mod telegram;


fn main() {
  pretty_env_logger::init();
  //  lightgbm::train();
  //  lightgbm::predict();
  //let _result = dataset::collect().await;
  let _ = alerts::subscribe();
  //telegram::start_bot().await;
}