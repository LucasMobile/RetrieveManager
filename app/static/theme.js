(() => {
  "use strict";

  const savedTheme = localStorage.getItem("rm-theme");
  const theme = savedTheme === "dark" ? "dark" : "light";

  document.documentElement.dataset.theme = theme;
  document
    .querySelector('meta[name="theme-color"]')
    ?.setAttribute("content", theme === "dark" ? "#0b1725" : "#071b33");
})();
