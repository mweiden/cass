use plotters::prelude::*;
use plotters::coord::Shift;
use regex::Regex;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Default)]
struct Metrics {
    op_rate: f64,
    row_rate: f64,
    mean_ms: f64,
    p95_ms: f64,
}

fn parse_number(s: &str) -> Option<f64> {
    let t = s.replace(',', "");
    t.parse::<f64>().ok()
}

fn parse_cassandra_stress_log(path: &str) -> Option<Metrics> {
    let content = fs::read_to_string(path).ok()?;
    let re_op = Regex::new(r"^\s*Op rate\s*:\s*([0-9.,]+)\s*op/s").ok()?;
    let re_row = Regex::new(r"^\s*Row rate\s*:\s*([0-9.,]+)\s*row/s").ok()?;
    let re_mean = Regex::new(r"^\s*Latency mean\s*:\s*([0-9.,]+)\s*ms").ok()?;
    let re_p95 = Regex::new(r"^\s*Latency 95th percentile\s*:\s*([0-9.,]+)\s*ms").ok()?;

    let mut m = Metrics::default();
    for line in content.lines() {
        if let Some(c) = re_op.captures(line) {
            if let Some(v) = parse_number(&c[1]) { m.op_rate = v; }
        }
        if let Some(c) = re_row.captures(line) {
            if let Some(v) = parse_number(&c[1]) { m.row_rate = v; }
        }
        if let Some(c) = re_mean.captures(line) {
            if let Some(v) = parse_number(&c[1]) { m.mean_ms = v; }
        }
        if let Some(c) = re_p95.captures(line) {
            if let Some(v) = parse_number(&c[1]) { m.p95_ms = v; }
        }
    }
    if m.row_rate == 0.0 { m.row_rate = m.op_rate; }
    if m.op_rate > 0.0 || m.mean_ms > 0.0 { Some(m) } else { None }
}

fn parse_cass_perf_log(path: &str) -> HashMap<String, Metrics> {
    let mut out: HashMap<String, Metrics> = HashMap::new();
    let content = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(_) => return out,
    };
    // Examples (both write and read):
    // Op rate                   : 1446 op/s  [WRITE: 1446 op/s]
    // Row rate                  : 1446 row/s [WRITE: 1446 row/s]
    // Latency mean              : 35.4 ms [WRITE: 35.4 ms]
    // Latency 95th percentile   : 76.7 ms [WRITE: 76.7 ms]
    let re_kind = Regex::new(r"\[(WRITE|READ)\]").unwrap();
    let re_op = Regex::new(r"^Op rate\s*:\s*([0-9.,]+)\s*op/s").unwrap();
    let re_row = Regex::new(r"^Row rate\s*:\s*([0-9.,]+)\s*row/s").unwrap();
    let re_mean = Regex::new(r"^Latency mean\s*:\s*([0-9.,]+)\s*ms").unwrap();
    let re_p95 = Regex::new(r"^Latency 95th percentile\s*:\s*([0-9.,]+)\s*ms").unwrap();

    let mut current_kind = String::new();
    for line in content.lines() {
        if let Some(c) = re_kind.captures(line) {
            current_kind = c[1].to_string();
            out.entry(current_kind.clone()).or_default();
        }
        if current_kind.is_empty() { continue; }
        let entry = out.entry(current_kind.clone()).or_default();
        if let Some(c) = re_op.captures(line) {
            if let Some(v) = parse_number(&c[1]) { entry.op_rate = v; }
        }
        if let Some(c) = re_row.captures(line) {
            if let Some(v) = parse_number(&c[1]) { entry.row_rate = v; }
        }
        if let Some(c) = re_mean.captures(line) {
            if let Some(v) = parse_number(&c[1]) { entry.mean_ms = v; }
        }
        if let Some(c) = re_p95.captures(line) {
            if let Some(v) = parse_number(&c[1]) { entry.p95_ms = v; }
        }
    }
    out
}

// removed old grouped bar plotting helper (not used by line plots)

fn draw_lines<B: DrawingBackend>(
    root: DrawingArea<B, Shift>,
    title: &str,
    y_label: &str,
    threads: &[i32],
    cass: &[f64],
    cassandra: &[f64],
) -> Result<(), Box<dyn std::error::Error>>
where
    B::ErrorType: 'static,
{
    let x_min = *threads.iter().min().unwrap_or(&0);
    let x_max = *threads.iter().max().unwrap_or(&0);
    let y_max = cass
        .iter()
        .chain(cassandra.iter())
        .cloned()
        .fold(0.0_f64, f64::max)
        * 1.15;
    root.fill(&WHITE)?;
    let mut chart = ChartBuilder::on(&root)
        .margin(10)
        .caption(title, ("sans-serif", 18).into_font())
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d(x_min..x_max, 0.0..y_max)?;

    chart
        .configure_mesh()
        .x_desc("threads")
        .y_desc(y_label)
        .x_labels(threads.len())
        .x_label_formatter(&|x| format!("{}", x))
        .disable_mesh()
        .draw()?;

    // cass line (blue)
    let series_a: Vec<(i32, f64)> = threads
        .iter()
        .cloned()
        .zip(cass.iter().cloned())
        .collect();
    chart
        .draw_series(LineSeries::new(series_a.clone(), BLUE.stroke_width(2)))?;
    chart.draw_series(series_a.iter().map(|(x, y)| Circle::new((*x, *y), 3, BLUE.filled())))?;

    // cassandra line (red)
    let series_b: Vec<(i32, f64)> = threads
        .iter()
        .cloned()
        .zip(cassandra.iter().cloned())
        .collect();
    chart
        .draw_series(LineSeries::new(series_b.clone(), RED.stroke_width(2)))?;
    chart.draw_series(series_b.iter().map(|(x, y)| TriangleMarker::new((*x, *y), 4, RED.filled())))?;

    // No per-panel legend; a single global legend is drawn by the caller

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let out_path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "perf-results/perf_comparison.png".to_string());
    let threads_arg = args.get(2).cloned().unwrap_or_else(|| "1 4 8 32 64".to_string());
    let thread_counts: Vec<i32> = threads_arg
        .split_whitespace()
        .filter_map(|s| s.parse::<i32>().ok())
        .collect();

    let res_dir = PathBuf::from("perf-results");

    // Collect metrics per thread count
    let mut w_cass_ops = Vec::new();
    let mut w_cassandra_ops = Vec::new();
    let mut r_cass_ops = Vec::new();
    let mut r_cassandra_ops = Vec::new();

    let mut w_cass_rows = Vec::new();
    let mut w_cassandra_rows = Vec::new();
    let mut r_cass_rows = Vec::new();
    let mut r_cassandra_rows = Vec::new();

    let mut w_cass_mean = Vec::new();
    let mut w_cassandra_mean = Vec::new();
    let mut r_cass_mean = Vec::new();
    let mut r_cassandra_mean = Vec::new();

    let mut w_cass_p95 = Vec::new();
    let mut w_cassandra_p95 = Vec::new();
    let mut r_cass_p95 = Vec::new();
    let mut r_cassandra_p95 = Vec::new();

    for t in &thread_counts {
        // our logs per t: cass_t{t}.log
        let cass_path = res_dir.join(format!("cass_t{}.log", t));
        let cass_map = parse_cass_perf_log(cass_path.to_str().unwrap_or(""));
        let c_w = cass_map.get("WRITE").cloned().unwrap_or_default();
        let c_r = cass_map.get("READ").cloned().unwrap_or_default();

        w_cass_ops.push(c_w.op_rate);
        r_cass_ops.push(c_r.op_rate);
        w_cass_rows.push(c_w.row_rate);
        r_cass_rows.push(c_r.row_rate);
        w_cass_mean.push(c_w.mean_ms);
        r_cass_mean.push(c_r.mean_ms);
        w_cass_p95.push(c_w.p95_ms);
        r_cass_p95.push(c_r.p95_ms);

        // cassandra logs per t
        let cs_w = parse_cassandra_stress_log(
            res_dir
                .join(format!("cassandra_write_t{}.log", t))
                .to_str()
                .unwrap_or("")
        )
        .unwrap_or_default();
        let cs_r = parse_cassandra_stress_log(
            res_dir
                .join(format!("cassandra_read_t{}.log", t))
                .to_str()
                .unwrap_or("")
        )
        .unwrap_or_default();

        w_cassandra_ops.push(cs_w.op_rate);
        r_cassandra_ops.push(cs_r.op_rate);
        w_cassandra_rows.push(cs_w.row_rate);
        r_cassandra_rows.push(cs_r.row_rate);
        w_cassandra_mean.push(cs_w.mean_ms);
        r_cassandra_mean.push(cs_r.mean_ms);
        w_cassandra_p95.push(cs_w.p95_ms);
        r_cassandra_p95.push(cs_r.p95_ms);
    }

    // Draw 2 rows x 4 columns (WRITE row, READ row) for metrics: ops, rows, mean, p95
    let root = BitMapBackend::new(&out_path, (1800, 960)).into_drawing_area();
    root.fill(&WHITE)?;
    // Reserve space at bottom for a global legend
    let legend_h: u32 = 50;
    let grid = root.margin(0, legend_h, 0, 0);
    let areas = grid.split_evenly((2, 4));

    draw_lines(
        areas[0].clone(),
        "WRITE: ops/s vs threads",
        "ops/s",
        &thread_counts,
        &w_cass_ops,
        &w_cassandra_ops,
    )?;
    draw_lines(
        areas[1].clone(),
        "WRITE: rows/s vs threads",
        "rows/s",
        &thread_counts,
        &w_cass_rows,
        &w_cassandra_rows,
    )?;
    draw_lines(
        areas[2].clone(),
        "WRITE: mean latency",
        "ms",
        &thread_counts,
        &w_cass_mean,
        &w_cassandra_mean,
    )?;
    draw_lines(
        areas[3].clone(),
        "WRITE: p95 latency",
        "ms",
        &thread_counts,
        &w_cass_p95,
        &w_cassandra_p95,
    )?;

    draw_lines(
        areas[4].clone(),
        "READ: ops/s vs threads",
        "ops/s",
        &thread_counts,
        &r_cass_ops,
        &r_cassandra_ops,
    )?;
    draw_lines(
        areas[5].clone(),
        "READ: rows/s vs threads",
        "rows/s",
        &thread_counts,
        &r_cass_rows,
        &r_cassandra_rows,
    )?;
    draw_lines(
        areas[6].clone(),
        "READ: mean latency",
        "ms",
        &thread_counts,
        &r_cass_mean,
        &r_cassandra_mean,
    )?;
    draw_lines(
        areas[7].clone(),
        "READ: p95 latency",
        "ms",
        &thread_counts,
        &r_cass_p95,
        &r_cassandra_p95,
    )?;

    // Global legend rendered at the bottom of the image
    let (w, h) = root.dim_in_pixel();
    let band_top = h as i32 - legend_h as i32;
    root.draw(&Rectangle::new(
        [(0, band_top), (w as i32, h as i32)],
        WHITE.mix(0.9).filled(),
    ))?;
    let x_center = (w as i32) / 2;
    let y = band_top + (legend_h as i32) / 2;
    // cass (blue)
    root.draw(&Rectangle::new([(x_center - 120, y - 8), (x_center - 108, y + 4)], BLUE.filled()))?;
    root.draw(&Text::new(
        "cass",
        (x_center - 100, y + 4),
        ("sans-serif", 16).into_font(),
    ))?;
    // cassandra (red)
    root.draw(&Rectangle::new([(x_center + 20, y - 8), (x_center + 32, y + 4)], RED.filled()))?;
    root.draw(&Text::new(
        "cassandra",
        (x_center + 40, y + 4),
        ("sans-serif", 16).into_font(),
    ))?;

    root.present()?;
    println!("Wrote {}", out_path);
    Ok(())
}
