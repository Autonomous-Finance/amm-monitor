package.loaded["subscriptions.subscriptions"] = nil
package.loaded["validation.validation"] = nil
package.loaded["validation.validation-schemas"] = nil
package.loaded["db.sqlschema"] = nil
package.loaded["db.seed"] = nil
package.loaded["db.utils"] = nil
package.loaded["dexi-core.dexi-core"] = nil
package.loaded["dexi-core.intervals"] = nil
package.loaded["dexi-core.candles"] = nil
package.loaded["dexi-core.stats"] = nil
package.loaded["dexi-core.overview"] = nil
package.loaded["dexi-core.price-around"] = nil
package.loaded["ingest.ingest"] = nil
package.loaded["indicators.indicators"] = nil
package.loaded["indicators.calc"] = nil
package.loaded["top-n.top-n"] = nil
package.loaded["utils.debug"] = nil
package.loaded["register-amm.register-amm"] = nil
package.loaded["integrate-amm.integrate-amm"] = nil
package.loaded["ops.emergency"] = nil
package.loaded["ops.config-ops"] = nil
package.loaded["ops.initialize"] = nil
package.loaded["amm-analytics.main"] = nil
package.loaded["amm-analytics.volume"] = nil
package.loaded["amm-analytics.pool-overview"] = nil


INITIAL_MODULES = { ".crypto.mac.hmac", "string", ".crypto.cipher.morus", "debug", ".handlers", ".crypto.padding.zero",
    ".crypto.digest.sha2_256", ".crypto.digest.md2", ".crypto.util.hex", ".default", ".eval", ".crypto.util.bit",
    ".utils", ".crypto.util.stream", "_G", "json", ".crypto.cipher.norx", ".base64", ".crypto.cipher.aes256",
    ".crypto.digest.md4", ".crypto.util.queue", ".stringify", ".handlers-utils", ".crypto.cipher.issac", "utf8",
    ".crypto.cipher.aes", ".dump", ".process", ".crypto.cipher.mode.cfb", "ao", ".pretty", ".crypto.digest.sha1",
    "coroutine", ".crypto.cipher.aes128", ".crypto.init", ".crypto.digest.sha2_512", ".crypto.cipher.aes192",
    ".crypto.kdf.pbkdf2", ".crypto.mac.init", ".crypto.digest.init", "package", "table", ".crypto.cipher.mode.ctr",
    ".crypto.util.array", "bit32", ".crypto.cipher.mode.ecb", ".crypto.kdf.init", ".assignment",
    ".crypto.cipher.mode.cbc", ".crypto.digest.blake2b", ".crypto.digest.sha3", ".crypto.digest.md5",
    ".crypto.cipher.mode.ofb", "io", "os", ".chance", ".crypto.util.init", ".crypto.cipher.init" }

for k, _ in pairs(package.loaded) do
    if not table.contains(INITIAL_MODULES, k) then
        package.loaded[k] = nil
    end
end

-- Helper function to check if a table contains a value
function table.contains(table, value)
    for _, v in pairs(table) do
        if v == value then
            return true
        end
    end
    return false
end
