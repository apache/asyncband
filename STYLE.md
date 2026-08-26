# Code Style

## Visibility

Express the widest intended visibility at the module boundary. Inside a restricted module such as a `pub(crate)` module, declare the items that form its API as `pub` instead of repeating `pub(crate)` on every item; the module boundary already limits their effective visibility. Use narrower visibility only for items that should not be available throughout the module's visible scope.
