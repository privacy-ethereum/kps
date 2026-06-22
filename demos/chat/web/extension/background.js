// Click the toolbar icon -> open the app in a new tab.
// We jump straight to the demo view via the #demo hash.
chrome.action.onClicked.addListener(() => {
  chrome.tabs.create({ url: chrome.runtime.getURL('index.html#demo') })
})
