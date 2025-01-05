use pyo3::{types::PyAnyMethods, Py, PyAny, Python};

fn main() {
    let table = vec![
        vec![90, 80, 75, 70],
        vec![35, 85, 55, 65],
        vec![125, 95, 90, 95],
        vec![45, 110, 95, 115],
        vec![50, 100, 90, 100],
    ];

    let d: Vec<(usize, usize)> = Python::with_gil(|py| {
        let entry = py.import("orstub.entry").unwrap();
        let entry: Py<PyAny> = entry.getattr("entry").unwrap().into();
        let res = entry.call1(py, (table,)).unwrap();
        res.extract(py).unwrap()
    });

    println!("{d:#?}");
}
