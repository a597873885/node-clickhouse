const uuid = require('node-uuid')
const fetch = require('node-fetch')
const { base64encode } = require('nodejs-base64');
const Utils = {
  getUuid() {
    return uuid.v1()
  },
  b64EncodeUnicode: function(tempStr) {
    const str = encodeURIComponent(tempStr)
    // return btoa(encodeURIComponent(str).replace(/%([0-9A-F]{2})/g, function(match, p1) {
    //   return String.fromCharCode("0x" + p1)
    // }))
    return base64encode(str)
  },
  uploadQueryTime(sql = "", time) {
    if (!sql) return
    const firstSqlWord = sql.split(" ")[0].toLowerCase()
    if (firstSqlWord === "create" || firstSqlWord === "insert") {
      return
    }

    let timeScope = "其他"
    if (time < 1000) {
      timeScope = "小于1s"
    } else if (time >= 1000 && time < 3000) {
      timeScope = "1~3s"
    } else if (time >= 3000 && time < 5000) {
      timeScope = "3~5s"
    } else if (time >= 5000 && time < 10000) {
      timeScope = "5~10s"
    } else if (time >= 10000 && time < 30000) {
      timeScope = "10~30s"
    } else if (time >= 30000) {
      timeScope = "大于30s"
    }
    fetch("http://monitor.webfunny.cn/tracker/upEvent", {
      method: "POST",
      body: JSON.stringify({
        pointId: "112",
        projectId: "event10338",
        chaXunHaoShi: time, // 查询耗时 | 类型：数值 | 描述：sql查询耗时，单位ms
        sqlyuJu: sql.replace(/ /g, "-"), // sql | 类型：文本 | 描述：sql语句
        haoShiFanWei: Utils.b64EncodeUnicode(timeScope), // 耗时范围 | 类型：文本 | 描述：耗时范围数据，便于统计查看
      }),
      headers: {
          "Content-Type": "text/plain;charset=UTF-8",
      }
    }).catch(function(e) {
        console.error(e)
    })
  }
}

module.exports = Utils