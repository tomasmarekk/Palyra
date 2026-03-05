import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const TEST_DIR = path.dirname(fileURLToPath(import.meta.url));
const CONTENT_SCRIPT_SOURCE = await readFile(
  path.join(TEST_DIR, "..", "content_script.js"),
  "utf8",
);

function createAttribute(name, value) {
  return { name, value };
}

function createTextNode(value) {
  return {
    nodeType: 3,
    nodeValue: value,
    childNodes: [],
    parentElement: null,
  };
}

function createElement(tagName, options = {}) {
  const { attributes = [], children = [] } = options;
  const element = {
    nodeType: 1,
    tagName: tagName.toUpperCase(),
    nodeName: tagName.toUpperCase(),
    attributes,
    childNodes: [],
    parentElement: null,
  };
  for (const child of children) {
    child.parentElement = element;
    element.childNodes.push(child);
  }
  return element;
}

function collectTextNodes(root) {
  const nodes = [];
  for (const child of Array.from(root?.childNodes || [])) {
    if (child.nodeType === 3) {
      nodes.push(child);
      continue;
    }
    nodes.push(...collectTextNodes(child));
  }
  return nodes;
}

function createTreeWalker(root) {
  const textNodes = collectTextNodes(root);
  let index = 0;
  return {
    nextNode() {
      const node = textNodes[index];
      index += 1;
      return node || null;
    },
  };
}

function loadContentScript({ documentElement, body, title = "Snapshot Test", url = "https://example.test/path" }) {
  let onMessageListener;
  const context = {
    TextEncoder,
    Node: {
      ELEMENT_NODE: 1,
      TEXT_NODE: 3,
      COMMENT_NODE: 8,
    },
    NodeFilter: {
      SHOW_TEXT: 4,
    },
    chrome: {
      runtime: {
        onMessage: {
          addListener(listener) {
            onMessageListener = listener;
          },
        },
      },
    },
    document: {
      documentElement,
      body,
      title,
      createTreeWalker(root, whatToShow) {
        assert.equal(whatToShow, 4);
        return createTreeWalker(root);
      },
    },
    window: {
      location: {
        href: url,
      },
    },
    console,
  };
  context.globalThis = context;

  vm.runInNewContext(CONTENT_SCRIPT_SOURCE, context, {
    filename: "content_script.js",
  });
  assert.equal(typeof onMessageListener, "function", "content script should register a message listener");

  return {
    dispatch(message) {
      let response;
      const returned = onMessageListener(message, null, (value) => {
        response = value;
      });
      assert.equal(returned, true, "content script listener should keep the response channel open");
      return response;
    },
  };
}

test("content script captures capped snapshots without touching outerHTML or innerText", () => {
  const body = createElement("body", {
    children: [
      createElement("main", {
        attributes: [createAttribute("data-role", "content")],
        children: [
          createTextNode("alpha ".repeat(12)),
          createElement("section", {
            children: [createTextNode("žluťoučký ".repeat(12))],
          }),
          createTextNode("omega ".repeat(12)),
        ],
      }),
    ],
  });
  const root = createElement("html", {
    children: [
      createElement("head", {
        children: [createElement("title", { children: [createTextNode("Snapshot Test")] })],
      }),
      body,
    ],
  });
  Object.defineProperty(root, "outerHTML", {
    get() {
      throw new Error("outerHTML must not be materialized");
    },
  });
  Object.defineProperty(body, "innerText", {
    get() {
      throw new Error("innerText must not be materialized");
    },
  });

  const script = loadContentScript({ documentElement: root, body });
  const response = script.dispatch({
    type: "palyra.collect_snapshot",
    maxDomBytes: 120,
    maxVisibleTextBytes: 48,
  });

  assert.equal(response.ok, true);
  assert.equal(response.page_url, "https://example.test/path");
  assert.equal(response.title, "Snapshot Test");
  assert.equal(Buffer.byteLength(response.dom_snapshot, "utf8") <= 120, true);
  assert.equal(Buffer.byteLength(response.visible_text, "utf8") <= 48, true);
  assert.equal(response.dom_truncated, true);
  assert.equal(response.visible_text_truncated, true);
  assert.match(response.dom_snapshot, /^<html>/);
  assert.match(response.visible_text, /^alpha alpha/);
});

test("content script preserves full snapshots when the page fits inside the budget", () => {
  const body = createElement("body", {
    children: [
      createElement("article", {
        attributes: [createAttribute("lang", "cs")],
        children: [createTextNode("Ahoj světe")],
      }),
    ],
  });
  const root = createElement("html", {
    children: [createElement("head"), body],
  });
  const script = loadContentScript({ documentElement: root, body, title: "Full Snapshot" });
  const response = script.dispatch({
    type: "palyra.collect_snapshot",
    maxDomBytes: 512,
    maxVisibleTextBytes: 128,
  });

  assert.equal(response.ok, true);
  assert.equal(response.dom_truncated, false);
  assert.equal(response.visible_text_truncated, false);
  assert.match(response.dom_snapshot, /<article lang="cs">Ahoj světe<\/article>/);
  assert.equal(response.visible_text, "Ahoj světe");
  assert.equal(response.title, "Full Snapshot");
});
