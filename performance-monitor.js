// Performance monitoring and throttling system
const { app, powerMonitor } = require('electron');
const os = require('os');

class PerformanceMonitor {
  constructor() {
    this.cpuUsageHistory = [];
    this.memoryUsageHistory = [];
    this.lastCleanup = Date.now();
    this.isThrottled = false;
    this.throttleReasons = new Set();
    
    // Initialize monitoring
    this.startMonitoring();
  }

  startMonitoring() {
    // Monitor system performance every 30 seconds
    setInterval(() => {
      this.updateSystemMetrics();
      this.checkThrottlingConditions();
    }, 30000);
  }

  updateSystemMetrics() {
    const now = Date.now();
    
    // Get CPU usage
    const cpuUsage = process.cpuUsage();
    const cpuPercent = this.calculateCPUPercent(cpuUsage);
    this.cpuUsageHistory.push({ timestamp: now, usage: cpuPercent });
    
    // Keep only last 10 minutes of history
    const tenMinutesAgo = now - 10 * 60 * 1000;
    this.cpuUsageHistory = this.cpuUsageHistory.filter(entry => entry.timestamp > tenMinutesAgo);
    
    // Get memory usage
    const memUsage = process.memoryUsage();
    const memPercent = (memUsage.heapUsed / memUsage.heapTotal) * 100;
    this.memoryUsageHistory.push({ timestamp: now, usage: memPercent });
    
    // Keep only last 10 minutes of history
    this.memoryUsageHistory = this.memoryUsageHistory.filter(entry => entry.timestamp > tenMinutesAgo);
  }

  calculateCPUPercent(cpuUsage) {
    // Simple CPU usage calculation
    const totalUsage = cpuUsage.user + cpuUsage.system;
    return Math.min(100, (totalUsage / 1000000) * 100); // Convert to percentage
  }

  checkThrottlingConditions() {
    this.throttleReasons.clear();
    
    // Check if CPU usage is consistently high
    if (this.cpuUsageHistory.length >= 3) {
      const recentCPU = this.cpuUsageHistory.slice(-3);
      const avgCPU = recentCPU.reduce((sum, entry) => sum + entry.usage, 0) / recentCPU.length;
      
      if (avgCPU > 80) {
        this.throttleReasons.add('high_cpu');
      }
    }
    
    // Check if memory usage is high
    if (this.memoryUsageHistory.length > 0) {
      const currentMem = this.memoryUsageHistory[this.memoryUsageHistory.length - 1].usage;
      if (currentMem > 92) { // Increased from 85 to 92 to reduce false positives during startup spikes
        this.throttleReasons.add('high_memory');
      }
    }
    
    // Check system idle time
    const idleTime = (powerMonitor && typeof powerMonitor.getSystemIdleTime === 'function') 
      ? powerMonitor.getSystemIdleTime() 
      : 0;
    
    if (idleTime < 20) { // Reduced from 30 to 20
      this.throttleReasons.add('user_active');
    }
    
    // Check battery/thermal state
    const onBattery = (powerMonitor && typeof powerMonitor.isOnBatteryPower === 'function') ? powerMonitor.isOnBatteryPower() : false;
    if (onBattery) {
      this.throttleReasons.add('on_battery');
    }
    
    const wasThrottled = this.isThrottled;
    const previousReasons = Array.from(this.throttleReasons || []).sort().join(',');
    
    this.isThrottled = this.throttleReasons.size > 0;
    const currentReasons = Array.from(this.throttleReasons).sort().join(',');

    if (this.isThrottled && (!wasThrottled || previousReasons !== currentReasons)) {
      console.log(`[Performance] Throttling active. Reasons: ${currentReasons}`);
    } else if (!this.isThrottled && wasThrottled) {
      console.log(`[Performance] Throttling lifted.`);
    }
  }

  shouldThrottleOperation(operation = 'default') {
    if (!this.isThrottled) return false;
    
    // Different operations have different throttling sensitivity
    const sensitiveOperations = ['screenshot', 'ocr', 'embedding'];
    const verySensitiveOperations = ['desktop_capture'];
    
    if (verySensitiveOperations.includes(operation)) {
      return this.throttleReasons.has('high_cpu') || this.throttleReasons.has('user_active');
    }
    
    if (sensitiveOperations.includes(operation)) {
      return this.throttleReasons.has('high_cpu') || this.throttleReasons.has('high_memory');
    }
    
    return this.throttleReasons.has('high_memory');
  }

  getRecommendedDelay(operation = 'default') {
    if (!this.shouldThrottleOperation(operation)) return 0;
    
    // Return delay in milliseconds based on throttling reasons
    let baseDelay = 0;
    
    if (this.throttleReasons.has('high_cpu')) {
      baseDelay += 60000; // 1 minute
    }
    
    if (this.throttleReasons.has('high_memory')) {
      baseDelay += 30000; // 30 seconds
    }
    
    if (this.throttleReasons.has('user_active')) {
      baseDelay += 15000; // 15 seconds
    }
    
    if (this.throttleReasons.has('on_battery')) {
      baseDelay += 45000; // 45 seconds
    }
    
    return baseDelay;
  }

  getPerformanceReport() {
    const now = Date.now();
    const recentCPU = this.cpuUsageHistory.slice(-5);
    const recentMemory = this.memoryUsageHistory.slice(-5);
    
    return {
      timestamp: now,
      isThrottled: this.isThrottled,
      throttleReasons: Array.from(this.throttleReasons),
      cpuUsage: recentCPU.length > 0 ? recentCPU[recentCPU.length - 1].usage : 0,
      memoryUsage: recentMemory.length > 0 ? recentMemory[recentMemory.length - 1].usage : 0,
      cpuHistory: recentCPU,
      memoryHistory: recentMemory
    };
  }
}

module.exports = new PerformanceMonitor();
