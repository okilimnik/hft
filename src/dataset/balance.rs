use image::imageops::rotate180_in_place;
use image::io::Reader as ImageReader;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use std::collections::HashMap;
use std::fs;

use crate::gcp;
