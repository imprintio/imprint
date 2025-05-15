use crate::types::{Order, Product};
use fake::Fake;
use fake::faker::company::en::*;
use fake::faker::lorem::en::*;
use uuid::Uuid;

pub fn mock_product(size: usize) -> Product {
    Product {
        id: Uuid::new_v4().to_string(),
        name: Words(size..(size * 2)).fake::<Vec<String>>().join(" "),
        description: Paragraph(size..(size * 2)).fake(),
        price: (10.0..1000.0).fake(),
        stock_quantity: (0..1000).fake(),
        category: Words(1..2).fake::<Vec<String>>().join(" "),
        brand: CompanyName().fake(),
        tags: Words(size * 2..size * 3).fake::<Vec<String>>(),
        is_active: true,
        sku: Uuid::new_v4().to_string(),
    }
}

pub fn mock_order(size: usize) -> Order {
    Order {
        id: Uuid::new_v4().to_string(),
        customer_id: Uuid::new_v4().to_string(),
        product_id: Uuid::new_v4().to_string(),
        quantity: (1..100).fake(),
        tags: Words(size..(size * 2)).fake::<Vec<String>>(),
    }
}
