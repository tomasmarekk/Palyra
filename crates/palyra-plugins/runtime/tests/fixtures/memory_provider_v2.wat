;; Reference candidate-only memory guest for executable ABI v2 conformance.
;; Its output schema contains proposals for host review and has no durable
;; write operation or journal authority.
(module
  (import "palyra:plugins/abi-v2@2" "emit-event" (func $emit-event (param i32 i32) (result i32)))
  (import "palyra:plugins/abi-v2@2" "is-cancelled" (func $is-cancelled (result i32)))
  (memory (export "memory") 4)
  (global $heap (mut i32) (i32.const 4096))
  (data (i32.const 768) "guest-event")
  (data (i32.const 1024) "{{OUTPUT}}")
  (func (export "palyra-abi-v2-alloc") (param $length i32) (result i32)
    (local $pointer i32)
    global.get $heap local.tee $pointer local.get $length i32.add global.set $heap
    local.get $pointer)
  (func (export "palyra-abi-v2-dealloc") (param i32 i32))
  (func (export "palyra-abi-v2-invoke")
    (param $request-pointer i32) (param $request-length i32)
    (param $output-pointer i32) (param $output-capacity i32) (result i32)
    local.get $request-length i32.const 16 i32.lt_u
    if i32.const -10 return end
    local.get $request-pointer i32.load8_u offset=10 i32.const 5 i32.ne
    if i32.const -10 return end
    i32.const 768 i32.const 11 call $emit-event drop
    call $is-cancelled
    if i32.const -2 return end
    local.get $output-capacity i32.const {{OUTPUT_LEN}} i32.lt_u
    if i32.const -3 return end
    local.get $output-pointer i32.const 1024 i32.const {{OUTPUT_LEN}} memory.copy
    i32.const {{OUTPUT_LEN}})
)
