(function () {
  const DEFAULT_PRICING_CONFIG = {
    monthly: {
      name: "VIP\u6708\u5ea6\u7248",
      price: 99,
      judgeCount: 50,
      convertCount: 50
    },
    addJudge: {
      name: "\u77ff\u5730\u5feb\u5224\u52a0\u6b21",
      price: 19,
      count: 10
    },
    addConvert: {
      name: "\u5750\u6807/KML\u52a0\u6b21",
      price: 19,
      count: 10
    },
    free: {
      dailyMax: 3,
      lifetimeMax: 12,
      timezone: "Asia/Shanghai"
    }
  };

  function toInteger(value, fallback) {
    const number = Number(value);
    if (!Number.isFinite(number) || number < 0) {
      return fallback;
    }
    return Math.floor(number);
  }

  function mergePricingConfig(source = {}) {
    const addPrice = toInteger(
      source.addPrice ?? source.addJudge?.price ?? source.addConvert?.price,
      DEFAULT_PRICING_CONFIG.addJudge.price
    );
    const addCount = toInteger(
      source.addCount ?? source.addJudge?.count ?? source.addConvert?.count,
      DEFAULT_PRICING_CONFIG.addJudge.count
    );

    return {
      monthly: {
        name: DEFAULT_PRICING_CONFIG.monthly.name,
        price: toInteger(source.monthly?.price, DEFAULT_PRICING_CONFIG.monthly.price),
        judgeCount: toInteger(source.monthly?.judgeCount, DEFAULT_PRICING_CONFIG.monthly.judgeCount),
        convertCount: toInteger(source.monthly?.convertCount, DEFAULT_PRICING_CONFIG.monthly.convertCount)
      },
      addJudge: {
        name: DEFAULT_PRICING_CONFIG.addJudge.name,
        price: addPrice,
        count: addCount
      },
      addConvert: {
        name: DEFAULT_PRICING_CONFIG.addConvert.name,
        price: addPrice,
        count: addCount
      },
      free: {
        dailyMax: toInteger(source.free?.dailyMax, DEFAULT_PRICING_CONFIG.free.dailyMax),
        lifetimeMax: toInteger(source.free?.lifetimeMax, DEFAULT_PRICING_CONFIG.free.lifetimeMax),
        timezone: "Asia/Shanghai"
      }
    };
  }

  const pricingTarget = window.PRICING_CONFIG || {};

  function applyPricingConfig(nextConfig) {
    const merged = mergePricingConfig(nextConfig);
    Object.keys(pricingTarget).forEach(key => delete pricingTarget[key]);
    Object.assign(pricingTarget, merged);
    window.PRICING_CONFIG = pricingTarget;
    window.dispatchEvent(new CustomEvent("pricing-config-updated", { detail: pricingTarget }));
    return pricingTarget;
  }

  async function loadPricingConfig() {
    const response = await fetch("/api/pricing-config", { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`Pricing config load failed: ${response.status}`);
    }
    const data = await response.json();
    return applyPricingConfig(data.config || data);
  }

  window.DEFAULT_PRICING_CONFIG = DEFAULT_PRICING_CONFIG;
  window.PRICING_CONFIG = applyPricingConfig(pricingTarget);
  window.applyPricingConfig = applyPricingConfig;
  window.loadPricingConfig = loadPricingConfig;
})();
