#[derive(Clone, Debug)]
pub enum BindingValue {
    Bool(bool),

    Byte(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),

    UByte(u8),
    SmallUInt(u16),
    UInt(u32),
    BigUInt(u64),

    Float(f32),
    Double(f64),

    Char(char),
    String(String),
}

#[derive(Clone, Debug)]
pub enum BindingType {
    Bool,
    Fixed,
    Real,
    Text,
}

impl ToString for BindingType {
    fn to_string(&self) -> String {
        match self {
            BindingType::Bool => "BOOLEAN",
            BindingType::Fixed => "FIXED",
            BindingType::Real => "REAL",
            BindingType::Text => "TEXT",
        }.into()
    }
}

impl From<BindingValue> for BindingType {
    fn from(value: BindingValue) -> Self {
        match value {
            BindingValue::Bool(_) => BindingType::Bool,
            BindingValue::Byte(_) |
            BindingValue::SmallInt(_) |
            BindingValue::Int(_) |
            BindingValue::BigInt(_) |
            BindingValue::UByte(_) |
            BindingValue::SmallUInt(_) |
            BindingValue::UInt(_) |
            BindingValue::BigUInt(_)
                => BindingType::Fixed,
            BindingValue::Float(_) |
            BindingValue::Double(_)
                => BindingType::Real,
            BindingValue::Char(_) |
            BindingValue::String(_)
                => BindingType::Text,
        }
    }
}

impl ToString for BindingValue {
    fn to_string(&self) -> String {
        match self {
            BindingValue::Bool(value) => value.to_string(),
            BindingValue::Byte(value) => value.to_string(),
            BindingValue::SmallInt(value) => value.to_string(),
            BindingValue::Int(value) => value.to_string(),
            BindingValue::BigInt(value) => value.to_string(),
            BindingValue::UByte(value) => value.to_string(),
            BindingValue::SmallUInt(value) => value.to_string(),
            BindingValue::UInt(value) => value.to_string(),
            BindingValue::BigUInt(value) => value.to_string(),
            BindingValue::Float(value) => value.to_string(),
            BindingValue::Double(value) => value.to_string(),
            BindingValue::Char(value) => value.to_string(),
            BindingValue::String(value) => value.to_string(),
        }
    }
}

impl From<&str> for BindingValue {
    fn from(value: &str) -> Self {
        BindingValue::String(value.to_owned())
    }
}

macro_rules! impl_from_binding_value {
    ($ty: ty, $ex: expr) => {
        impl From<$ty> for BindingValue {
            fn from(value: $ty) -> Self {
                $ex(value)
            }
        }
    };
}
impl_from_binding_value!(bool, BindingValue::Bool);
impl_from_binding_value!(i8, BindingValue::Byte);
impl_from_binding_value!(i16, BindingValue::SmallInt);
impl_from_binding_value!(i32, BindingValue::Int);
impl_from_binding_value!(i64, BindingValue::BigInt);
impl_from_binding_value!(u8, BindingValue::UByte);
impl_from_binding_value!(u16, BindingValue::SmallUInt);
impl_from_binding_value!(u32, BindingValue::UInt);
impl_from_binding_value!(u64, BindingValue::BigUInt);
impl_from_binding_value!(f32, BindingValue::Float);
impl_from_binding_value!(f64, BindingValue::Double);
impl_from_binding_value!(char, BindingValue::Char);
impl_from_binding_value!(String, BindingValue::String);
