//! Streaming bounded wire primitives for selection-controlled collections and labels.
//!
//! Deserialization rejects oversized `size_hint` values before allocation and
//! stops at the declared limit even when an untrusted serializer omits its hint.

use std::{
    fmt,
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use serde::{
    de::{Error as _, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};

const MAX_SAFE_LABEL_BYTES: usize = 128;

/// A bounded vector whose wire decoder never allocates beyond `MAX`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub(crate) struct BoundedVec<T, const MAX: usize>(Vec<T>);

impl<T, const MAX: usize> BoundedVec<T, MAX> {
    /// Validates and wraps an owned vector.
    ///
    /// # Errors
    /// Returns the original vector when it exceeds `MAX`.
    pub(crate) fn try_new(values: Vec<T>) -> Result<Self, Vec<T>> {
        if values.len() > MAX {
            Err(values)
        } else {
            Ok(Self(values))
        }
    }

    /// Returns an empty bounded vector.
    #[must_use]
    pub(crate) const fn empty() -> Self {
        Self(Vec::new())
    }
}

impl<T, const MAX: usize> Default for BoundedVec<T, MAX> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<T, const MAX: usize> Deref for BoundedVec<T, MAX> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.0.as_slice()
    }
}

impl<T, const MAX: usize> DerefMut for BoundedVec<T, MAX> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut_slice()
    }
}

impl<'de, T, const MAX: usize> Deserialize<'de> for BoundedVec<T, MAX>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BoundedVecVisitor<T, const MAX: usize>(PhantomData<T>);

        impl<'de, T, const MAX: usize> Visitor<'de> for BoundedVecVisitor<T, MAX>
        where
            T: Deserialize<'de>,
        {
            type Value = BoundedVec<T, MAX>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a sequence with at most {MAX} entries")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                if sequence.size_hint().is_some_and(|hint| hint > MAX) {
                    return Err(A::Error::custom(format_args!("sequence exceeds {MAX} entries")));
                }
                let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAX));
                while let Some(value) = sequence.next_element()? {
                    if values.len() == MAX {
                        return Err(A::Error::custom(format_args!(
                            "sequence exceeds {MAX} entries"
                        )));
                    }
                    values.push(value);
                }
                Ok(BoundedVec(values))
            }
        }

        deserializer.deserialize_seq(BoundedVecVisitor::<T, MAX>(PhantomData))
    }
}

/// A bounded low-cardinality identifier or capability label.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub(crate) struct SafeLabel(String);

impl fmt::Debug for SafeLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("SafeLabel").field(&self.0).finish()
    }
}

impl<'de> Deserialize<'de> for SafeLabel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(D::Error::custom)
    }
}

impl SafeLabel {
    /// Parses a bounded label containing only durable identifier punctuation.
    ///
    /// # Errors
    /// Returns a static reason when the label is empty, oversized, or unsafe.
    pub(crate) fn parse(value: String) -> Result<Self, &'static str> {
        if value.is_empty()
            || value.len() > MAX_SAFE_LABEL_BYTES
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
            })
        {
            return Err("selection label is invalid");
        }
        Ok(Self(value))
    }

    /// Returns the validated label.
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}
