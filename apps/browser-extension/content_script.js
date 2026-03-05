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

  const ELEMENT_NODE = typeof Node !== "undefined" ? Node.ELEMENT_NODE : 1;
  const TEXT_NODE = typeof Node !== "undefined" ? Node.TEXT_NODE : 3;
  const COMMENT_NODE = typeof Node !== "undefined" ? Node.COMMENT_NODE : 8;
  const SHOW_TEXT = typeof NodeFilter !== "undefined" ? NodeFilter.SHOW_TEXT : 4;
  const VOID_ELEMENTS = new Set([
    "area",
    "base",
    "br",
    "col",
    "embed",
    "hr",
    "img",
    "input",
    "link",
    "meta",
    "param",
    "source",
    "track",
    "wbr",
  ]);

  function createCappedBuffer(maxBytes) {
    return {
      maxBytes: Number.isFinite(maxBytes) && maxBytes > 0 ? Math.floor(maxBytes) : 0,
      bytes: 0,
      parts: [],
      truncated: false,
    };
  }

  function appendCappedFragment(buffer, fragment) {
    if (buffer.truncated || !fragment) {
      return;
    }
    const remaining = buffer.maxBytes - buffer.bytes;
    if (remaining <= 0) {
      buffer.truncated = true;
      return;
    }
    const capped = clampUtf8Bytes(fragment, remaining);
    if (capped.value) {
      buffer.parts.push(capped.value);
      buffer.bytes += utf8ByteLength(capped.value);
    }
    if (capped.truncated) {
      buffer.truncated = true;
    }
  }

  function finishCappedBuffer(buffer) {
    return {
      value: buffer.parts.join(""),
      truncated: buffer.truncated,
    };
  }

  function escapeHtmlText(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;");
  }

  function escapeHtmlAttribute(value) {
    return escapeHtmlText(value).replaceAll('"', "&quot;");
  }

  function collectDomSnapshotCapped(root, maxBytes) {
    if (!root) {
      return { value: "", truncated: false };
    }
    const buffer = createCappedBuffer(maxBytes);
    const stack = [{ node: root, closingTag: "" }];
    while (stack.length > 0 && !buffer.truncated) {
      const current = stack.pop();
      if (!current) {
        continue;
      }
      if (current.closingTag) {
        appendCappedFragment(buffer, current.closingTag);
        continue;
      }

      const node = current.node;
      if (!node) {
        continue;
      }

      if (node.nodeType === ELEMENT_NODE) {
        const tagName = String(node.tagName || node.nodeName || "div").toLowerCase();
        let openingTag = `<${tagName}`;
        for (const attribute of Array.from(node.attributes || [])) {
          if (!attribute || !attribute.name) {
            continue;
          }
          openingTag += ` ${attribute.name}="${escapeHtmlAttribute(attribute.value || "")}"`;
        }
        openingTag += ">";
        appendCappedFragment(buffer, openingTag);
        if (buffer.truncated || VOID_ELEMENTS.has(tagName)) {
          continue;
        }
        stack.push({ node: null, closingTag: `</${tagName}>` });
        const children = Array.from(node.childNodes || []);
        for (let index = children.length - 1; index >= 0; index -= 1) {
          stack.push({ node: children[index], closingTag: "" });
        }
        continue;
      }

      if (node.nodeType === TEXT_NODE) {
        appendCappedFragment(buffer, escapeHtmlText(node.nodeValue || ""));
        continue;
      }

      if (node.nodeType === COMMENT_NODE) {
        appendCappedFragment(buffer, `<!--${node.nodeValue || ""}-->`);
      }
    }
    return finishCappedBuffer(buffer);
  }

  function normalizeVisibleTextFragment(value) {
    return String(value || "").replace(/\s+/g, " ").trim();
  }

  function collectVisibleTextCapped(body, maxBytes) {
    if (!body || typeof document?.createTreeWalker !== "function") {
      return { value: "", truncated: false };
    }
    const buffer = createCappedBuffer(maxBytes);
    const walker = document.createTreeWalker(body, SHOW_TEXT);
    let requiresSeparator = false;
    while (!buffer.truncated) {
      const nextNode = walker.nextNode();
      if (!nextNode) {
        break;
      }
      const fragment = normalizeVisibleTextFragment(nextNode.nodeValue || "");
      if (!fragment) {
        continue;
      }
      if (requiresSeparator) {
        appendCappedFragment(buffer, " ");
      }
      appendCappedFragment(buffer, fragment);
      requiresSeparator = true;
    }
    return finishCappedBuffer(buffer);
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
    const dom = collectDomSnapshotCapped(root, maxDomBytes);
    const visibleText = collectVisibleTextCapped(body, maxVisibleTextBytes);

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
