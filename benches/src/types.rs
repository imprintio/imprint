use imprint::{ImprintRecord, ImprintWriter, SchemaId};

include!(concat!(env!("OUT_DIR"), "/test.rs"));

impl Product {
    pub fn to_imprint(&self) -> ImprintRecord {
        let mut writer = ImprintWriter::new(SchemaId {
            fieldspace_id: 0,
            schema_hash: 0,
        })
        .unwrap();

        writer.add_field(1, self.id.clone().into()).unwrap();
        writer.add_field(2, self.name.clone().into()).unwrap();
        writer
            .add_field(3, self.description.clone().into())
            .unwrap();
        writer.add_field(4, self.price.into()).unwrap();
        writer.add_field(5, self.stock_quantity.into()).unwrap();
        writer.add_field(6, self.category.clone().into()).unwrap();
        writer.add_field(7, self.brand.clone().into()).unwrap();
        writer
            .add_field(
                8,
                self.tags
                    .iter()
                    .map(|t| t.clone())
                    .collect::<Vec<_>>()
                    .into(),
            )
            .unwrap();
        writer.add_field(9, self.is_active.into()).unwrap();
        writer.add_field(10, self.sku.clone().into()).unwrap();

        writer.build().unwrap()
    }
}

impl Order {
    pub fn to_imprint(&self) -> ImprintRecord {
        let mut writer = ImprintWriter::new(SchemaId {
            fieldspace_id: 0,
            schema_hash: 1,
        })
        .unwrap();

        writer.add_field(101, self.id.clone().into()).unwrap();
        writer
            .add_field(102, self.customer_id.clone().into())
            .unwrap();
        writer
            .add_field(103, self.product_id.clone().into())
            .unwrap();
        writer.add_field(104, self.quantity.into()).unwrap();
        writer
            .add_field(
                105,
                self.tags
                    .iter()
                    .map(|t| t.clone())
                    .collect::<Vec<_>>()
                    .into(),
            )
            .unwrap();

        writer.build().unwrap()
    }
}
