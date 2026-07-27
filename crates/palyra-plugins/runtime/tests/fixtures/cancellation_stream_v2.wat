;; Streaming cancellation guest used to prove event-before-terminal ordering.
;; After publishing one event it waits cooperatively on the host cancellation
;; import, keeping cancellation deterministic without wall-clock sleeps.
(module
  (import "palyra:plugins/abi-v2@2" "emit-event" (func $emit-event (param i32 i32) (result i32)))
  (import "palyra:plugins/abi-v2@2" "is-cancelled" (func $is-cancelled (result i32)))
  (memory (export "memory") 4)
  (global $heap (mut i32) (i32.const 4096))
  (data (i32.const 768) "before-cancel")
  (func (export "palyra-abi-v2-alloc") (param $length i32) (result i32)
    (local $pointer i32)
    global.get $heap local.tee $pointer local.get $length i32.add global.set $heap
    local.get $pointer)
  (func (export "palyra-abi-v2-dealloc") (param i32 i32))
  (func (export "palyra-abi-v2-invoke")
    (param i32) (param i32) (param i32) (param i32) (result i32)
    i32.const 768
    i32.const 13
    call $emit-event
    drop
    loop $wait
      call $is-cancelled
      i32.eqz
      br_if $wait
    end
    i32.const -2)
)
