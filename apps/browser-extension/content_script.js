(() => {
  if (window.__palyraRelayContentScriptInstalled) {
    return;
  }
  window.__palyraRelayContentScriptInstalled = true;

  function utf8ByteLength(value) {
    return new TextEncoder().encode(value).length;
  }

  function clampUtf8Bytes(value, maxBytes) {
    if (!Number.isFinite(maxBytes) || maxBytes <= 0) {
      return { value: "", truncated: value.length > 0 };
    }
    if (utf8ByteLength(value) <= maxBytes) {
      return { value, truncated: false };
    }
    let low = 0;
    let high = value.length;
    let best = "";
    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      const candidate = value.slice(0, mid);
      if (utf8ByteLength(candidate) <= maxBytes) {
        best = candidate;
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return { value: best, truncated: true };
  }

  chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
    if (!message || message.type !== "palyra.collect_snapshot") {
      return;
    }
    const maxDomBytes = Number.isFinite(message.maxDomBytes) ? message.maxDomBytes : 16 * 1024;
    const maxVisibleTextBytes = Number.isFinite(message.maxVisibleTextBytes)
      ? message.maxVisibleTextBytes
      : 8 * 1024;

    const root = document.documentElement;
    const body = document.body;
    const domRaw = root ? root.outerHTML : "";
    const visibleRaw = body ? body.innerText || "" : "";
    const dom = clampUtf8Bytes(domRaw, maxDomBytes);
    const visibleText = clampUtf8Bytes(visibleRaw, maxVisibleTextBytes);

    sendResponse({
      ok: true,
      page_url: window.location.href,
      title: document.title,
      dom_snapshot: dom.value,
      visible_text: visibleText.value,
      dom_truncated: dom.truncated,
      visible_text_truncated: visibleText.truncated,
    });
    return true;
  });
})();
