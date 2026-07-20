# PCRE2 system dependency

`libdagforge-jsonata` uses the system PCRE2 8-bit library behind `regex_adapter.cpp`. DAGForge requires PCRE2 10.40 or newer and verifies the limit APIs during build configuration. PCRE2 types are private to the adapter and are not exported by DAGForge headers or the main runtime library.

The PCRE2 license is reproduced in `third_party/pcre2/LICENSE` for binary-distribution compliance.
