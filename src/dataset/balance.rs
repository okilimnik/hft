use image::imageops::rotate180_in_place;
use image::io::Reader as ImageReader;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use std::collections::HashMap;
use std::fs;

use crate::gcp;

fn rotate(filename: String, new_filename: String) {
    let mut img = ImageReader::open(format!("./{}", filename))
        .unwrap()
        .decode()
        .unwrap();
    rotate180_in_place(&mut img);
    img.save(format!("./{}", new_filename)).unwrap();
}
