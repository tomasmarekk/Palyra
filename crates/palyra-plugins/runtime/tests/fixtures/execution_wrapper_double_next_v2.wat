;; Adversarial execution-wrapper guest for executable ABI v2 conformance.
;; Calling the host-owned continuation twice must fail the whole invocation
;; even when the guest otherwise returns a valid typed result.
(module
  (import "palyra:plugins/abi-v2@2" "next-call" (func $next-call (param i32 i32) (result i32)))
  (memory (export "memory") 4)
  (global $heap (mut i32) (i32.const 4096))
  (data (i32.const 768) "{{CALL_ID}}")
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
    i32.const 768 i32.const {{CALL_ID_LEN}} call $next-call drop
    i32.const 768 i32.const {{CALL_ID_LEN}} call $next-call drop
    local.get $output-capacity i32.const {{OUTPUT_LEN}} i32.lt_u
    if i32.const -3 return end
    local.get $output-pointer i32.const 1024 i32.const {{OUTPUT_LEN}} memory.copy
    i32.const {{OUTPUT_LEN}})
)
