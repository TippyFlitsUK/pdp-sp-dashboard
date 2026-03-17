// Simple TTL cache — Map-based, no deps
class Cache {
  constructor() {
    this.store = new Map()
  }

  get(key) {
    const entry = this.store.get(key)
    if (!entry) return null
    if (Date.now() > entry.expires) {
      this.store.delete(key)
      return null
    }
    return entry.value
  }

  set(key, value, ttlMs) {
    this.store.set(key, { value, expires: Date.now() + ttlMs })
  }

  delete(key) {
    this.store.delete(key)
  }

  clear() {
    this.store.clear()
  }
}

module.exports = { Cache }
