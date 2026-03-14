import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App } from "./App";
import "./styles.css";

const container = document.getElementById("root");

if (container === null) {
  throw new Error("Missing #root container for web console bootstrap.");
}

const rootElement = document.documentElement;
rootElement.dataset.theme = "dark";
rootElement.classList.add("dark");
rootElement.style.colorScheme = "dark";

createRoot(container).render(
  <StrictMode>
    <App />
  </StrictMode>
);
