use imprint::{ImprintError, ImprintRecord, ImprintWriter, SchemaId, Value};

include!(concat!(env!("OUT_DIR"), "/test.rs"));

impl Product {
    pub fn to_imprint(&self) -> Result<ImprintRecord, ImprintError> {
        let mut writer = ImprintWriter::new(SchemaId {
            fieldspace_id: 0,
            schema_hash: 0,
        })
        .unwrap();

        writer.add_field(1, Value::String(self.id.clone()))?;
        writer.add_field(2, Value::String(self.name.clone()))?;
        writer.add_field(3, Value::String(self.description.clone()))?;
        writer.add_field(4, Value::Float64(self.price))?;
        writer.add_field(5, Value::Int32(self.stock_quantity))?;
        writer.add_field(6, Value::String(self.category.clone()))?;
        writer.add_field(7, Value::String(self.brand.clone()))?;
        writer.add_field(
            8,
            Value::Array(self.tags.iter().map(|t| Value::String(t.clone())).collect()),
        )?;
        writer.add_field(9, Value::Bool(self.is_active))?;
        writer.add_field(10, Value::String(self.sku.clone()))?;

        Ok(writer.build()?)
    }
}

impl Order {
    pub fn to_imprint(&self) -> Result<ImprintRecord, ImprintError> {
        let mut writer = ImprintWriter::new(SchemaId {
            fieldspace_id: 0,
            schema_hash: 1,
        })?;

        writer.add_field(101, Value::String(self.id.clone()))?;
        writer.add_field(102, Value::String(self.customer_id.clone()))?;
        writer.add_field(103, Value::String(self.product_id.clone()))?;
        writer.add_field(104, Value::Int32(self.quantity))?;
        writer.add_field(
            105,
            Value::Array(self.tags.iter().map(|t| Value::String(t.clone())).collect()),
        )?;

        Ok(writer.build()?)
    }
}
