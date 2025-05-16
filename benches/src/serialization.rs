mod mock_data;
mod types;

use bytes::BytesMut;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use imprint::{ImprintRecord, Merge, Project, Read, Write};
use prost::Message;
use types::{EnrichedOrder, Order, Product, SimpleProduct};

fn benchmark_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialize");
    let product = mock_data::mock_product(5);

    // Baseline (Protobuf Serde)
    group.bench_function("protobuf_serialize", |b| {
        b.iter(|| {
            let mut buf = Vec::new();
            product.encode(&mut buf).unwrap();
            black_box(buf);
        })
    });

    let imprint_product = product.to_imprint().unwrap();
    group.bench_function("imprint_serialize", |b| {
        b.iter(|| {
            let mut buf = BytesMut::new();
            imprint_product.write(&mut buf).unwrap();
            black_box(buf);
        })
    });

    group.finish();
}

fn benchmark_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("deserialize");
    let product = mock_data::mock_product(5);
    let mut buf = Vec::new();
    product.encode(&mut buf).unwrap();

    group.bench_function("protobuf_deserialize", |b| {
        b.iter(|| {
            let product = Product::decode(&buf[..]).unwrap();
            black_box(product);
        })
    });

    let imprint_product = product.to_imprint().unwrap();
    let mut buf = BytesMut::new();
    imprint_product.write(&mut buf).unwrap();

    group.bench_function("imprint_deserialize", |b| {
        b.iter(|| {
            let product = ImprintRecord::read(buf.clone().freeze()).unwrap();
            black_box(product);
        })
    });
    group.finish();
}

fn benchmark_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge");

    for size in [1, 5, 10].iter() {
        let product = mock_data::mock_product(*size);
        let order = mock_data::mock_order(*size);

        let product_buf = product.encode_to_vec();
        let order_buf = order.encode_to_vec();

        group.bench_function(format!("protobuf_merge_size_{}", size), |b| {
            b.iter(|| {
                let product = Product::decode(&product_buf[..]).unwrap();
                let order = Order::decode(&order_buf[..]).unwrap();

                let enriched = EnrichedOrder {
                    order: Some(order.clone()),
                    product: Some(product.clone()),
                };

                black_box(enriched.encode_to_vec());
            })
        });

        let product_imprint = product.to_imprint().unwrap();
        let order_imprint = order.to_imprint().unwrap();

        let mut product_buf = BytesMut::new();
        product_imprint.write(&mut product_buf).unwrap();
        let mut order_buf = BytesMut::new();
        order_imprint.write(&mut order_buf).unwrap();

        group.bench_function(format!("imprint_merge_size_{}", size), |b| {
            b.iter(|| {
                let (product, _) = ImprintRecord::read(product_buf.clone().freeze()).unwrap();
                let (order, _) = ImprintRecord::read(order_buf.clone().freeze()).unwrap();

                let enriched = product.merge(&order).unwrap();
                let mut buf = BytesMut::new();
                enriched.write(&mut buf).unwrap();
                black_box(buf);
            })
        });
    }

    group.finish();
}

fn benchmark_project(c: &mut Criterion) {
    let mut group = c.benchmark_group("project");

    for size in [1, 5, 10].iter() {
        let product = mock_data::mock_product(*size);
        let product_buf = product.encode_to_vec();

        group.bench_function(format!("protobuf_project_size_{}", size), |b| {
            b.iter(|| {
                let product = Product::decode(&product_buf[..]).unwrap();
                let simple = SimpleProduct {
                    id: product.id,
                    name: product.name,
                    price: product.price,
                    category: product.category,
                    brand: product.brand,
                };
                black_box(simple.encode_to_vec());
            })
        });

        let product_imprint = product.to_imprint().unwrap();
        let mut product_buf = BytesMut::new();
        product_imprint.write(&mut product_buf).unwrap();

        group.bench_function(format!("imprint_project_size_{}", size), |b| {
            b.iter(|| {
                let (product, _) = ImprintRecord::read(product_buf.clone().freeze()).unwrap();
                let projected = product.project(&[1, 2, 3, 4, 6, 7]).unwrap();
                let mut buf = BytesMut::new();
                projected.write(&mut buf).unwrap();
                black_box(buf);
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_serialize,
    benchmark_deserialize,
    benchmark_merge,
    benchmark_project
);
criterion_main!(benches);
