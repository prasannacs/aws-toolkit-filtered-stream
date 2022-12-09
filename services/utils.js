function sleep(milliseconds) {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve('timed');
        }, milliseconds)
    })
}

function cleanseText(text)  {
    text = text.replace(/(\r\n|\n|\r)/gm, "");
    text = text.split('|').join(' ');
    return text;
}

module.exports = { sleep, cleanseText };
