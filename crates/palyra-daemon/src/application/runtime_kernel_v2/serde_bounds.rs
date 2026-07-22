// Bounded deserialization helpers for durable kernel collections.

fn deserialize_bounded_vec<'de, D, T, const LIMIT: usize>(
    deserializer: D,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BoundedVecVisitor<T, const LIMIT: usize>(PhantomData<T>);

    impl<'de, T, const LIMIT: usize> Visitor<'de> for BoundedVecVisitor<T, LIMIT>
    where
        T: Deserialize<'de>,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "a sequence with at most {LIMIT} entries")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let capacity = sequence.size_hint().unwrap_or_default().min(LIMIT);
            let mut values = Vec::with_capacity(capacity);
            while let Some(value) = sequence.next_element()? {
                if values.len() == LIMIT {
                    return Err(A::Error::invalid_length(
                        values.len() + 1,
                        &"a bounded runtime-kernel sequence",
                    ));
                }
                values.push(value);
            }
            Ok(values)
        }
    }

    deserializer.deserialize_seq(BoundedVecVisitor::<T, LIMIT>(PhantomData))
}

fn deserialize_lane_leases<'de, D>(deserializer: D) -> Result<Vec<GenerationLeaseV1>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec::<D, GenerationLeaseV1, MAX_KERNEL_EVENT_CURSORS>(deserializer)
}

fn deserialize_event_cursors<'de, D>(deserializer: D) -> Result<Vec<KernelEventCursor>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec::<D, KernelEventCursor, MAX_KERNEL_EVENT_CURSORS>(deserializer)
}
