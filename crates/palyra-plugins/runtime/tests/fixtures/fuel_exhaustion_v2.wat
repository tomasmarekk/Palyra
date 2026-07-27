;; Hostile executable ABI v2 guest used to prove fuel exhaustion is terminal.
;; It provides every required memory export but never returns from invocation.
(module
  (import "palyra:plugins/abi-v2@2" "emit-event" (func (param i32 i32) (result i32)))
  (import "palyra:plugins/abi-v2@2" "is-cancelled" (func (result i32)))
  (memory (export "memory") 4)
  (global $heap (mut i32) (i32.const 4096))
  (func (export "palyra-abi-v2-alloc") (param $length i32) (result i32)
    (local $pointer i32)
    global.get $heap local.tee $pointer local.get $length i32.add global.set $heap
    local.get $pointer)
  (func (export "palyra-abi-v2-dealloc") (param i32 i32))
  (func (export "palyra-abi-v2-invoke")
    (param i32) (param i32) (param i32) (param i32) (result i32)
    loop $forever
      br $forever
    end
    i32.const 0)
)
