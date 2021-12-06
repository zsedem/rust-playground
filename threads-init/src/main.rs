//Copyright 2017 Adam Oliver Zsigmond
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
// of the Software, and to permit persons to whom the Software is furnished to do
// so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
use std::thread;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::time::Duration;

fn main() {
    let (tx, rx) = mpsc::channel();

    start_multiple_threads(tx);

    for received in rx {
        println!("Got: {}", received);
    }
}

fn start_multiple_threads(tx: Sender<String>) {
    for i in 1..10 {
        start_thread(i, &tx)
    }
}

fn start_thread(i: i32, tx1: &Sender<String>) {
    let tx = tx1.clone();
    thread::spawn(move || {
        let vals = vec![
            String::from("more"),
            String::from("messages"),
            String::from("for"),
            String::from("you"),
        ];

        for val in vals.iter().map(|x| format!("thread {}: {}", x, i)) {
            tx.send(val).unwrap();
            thread::sleep(Duration::from_secs(1));
        }
    });
}
