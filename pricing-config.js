(function () {
  const PRICING_CONFIG = {
    monthly: {
      name: "月度版",
      price: 99,
      judgeCount: 50,
      convertCount: 50
    },
    addJudge: {
      name: "矿地快判加次",
      price: 19,
      count: 10
    },
    addConvert: {
      name: "坐标/KML加次",
      price: 19,
      count: 10
    }
  };

  window.PRICING_CONFIG = PRICING_CONFIG;
})();
