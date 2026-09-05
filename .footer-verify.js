const http = require('http')
http.get('http://127.0.0.1:9226/json/list', (response) => {
  let body = ''
  response.on('data', (chunk) => { body += chunk })
  response.on('end', () => {
    const page = JSON.parse(body).find((item) => item.type === 'page')
    const socket = new WebSocket(page.webSocketDebuggerUrl)
    socket.onopen = () => socket.send(JSON.stringify({
      id: 1,
      method: 'Runtime.evaluate',
      params: {
        expression: `(() => {
          const footer = document.querySelector('footer.footer')
          const rect = footer.getBoundingClientRect()
          const css = getComputedStyle(footer)
          return JSON.stringify({
            position: css.position,
            top: Math.round(rect.top),
            bottom: Math.round(rect.bottom),
            height: Math.round(rect.height),
            innerHeight,
            text: footer.textContent.trim(),
            productDoc: document.body.textContent.includes('独立产品文档'),
          })
        })()`,
        returnByValue: true,
      },
    }))
    socket.onmessage = (event) => {
      const message = JSON.parse(event.data)
      if (message.id === 1) {
        console.log(message.result.result.value)
        socket.close()
      }
    }
  })
})
