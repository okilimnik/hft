use log::debug;

use crate::{
    dataset::{order_book::OrderBookState, utils},
    lightgbm,
};

pub fn trade(data: Vec<OrderBookState>) {
    let svm_row = utils::to_svm(0, data);
    let prediction = lightgbm::predict(svm_row);
    debug!("Prediction is {:.3}", prediction);
}
