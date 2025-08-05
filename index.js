const express = require('express');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { getWeek, getMonth, getQuarter } = require('date-fns');
const cors = require('cors')

const app = express();

// Enable CORS for all routes
app.use(cors());

// Middleware to log all incoming requests
app.use((req, res, next) => {
    console.log(`Received request: ${req.method} ${req.url}`);
    next();
});
const port = 3000;

const dataPath = path.join(__dirname, 'Data', 'all_pump_forecasts.csv');
const insightsDataPath = path.join(__dirname, 'Data', 'groq_bullet_monthly_insights.csv');
let forecastData = [];
let insightsData = [];

// Function to read and parse the CSV data
const loadData = () => {
    return new Promise((resolve, reject) => {
        const data = [];
        fs.createReadStream(dataPath)
            .pipe(csv())
            .on('data', (row) => {
                row.Forecasted_Demand = parseFloat(row.Forecasted_Demand);
                row.Date = new Date(row.Date);
                data.push(row);
            })
            .on('end', () => {
                forecastData = data;
                console.log('CSV data successfully loaded.');
                resolve(forecastData);
            })
            .on('error', (error) => {
                console.error('Error loading CSV data:', error);
                reject(error);
            });
    });
};

// Helper function to filter data based on query parameters
const filterData = (req) => {
    const { PRODUCT_CARD_ID, PRODUCT_NAME } = req.query;
    let filteredData = [...forecastData];

    if (PRODUCT_CARD_ID) {
        filteredData = filteredData.filter(d => d.PRODUCT_CARD_ID === PRODUCT_CARD_ID);
    }
    if (PRODUCT_NAME) {
        filteredData = filteredData.filter(d => d.PRODUCT_NAME === PRODUCT_NAME);
    }
    return filteredData;
};

// API endpoint to get all forecast data (with optional filtering)
app.get('/api/forecasts', (req, res) => {
    console.log('Handling request for /api/forecasts');
    const filteredData = filterData(req);
    res.json(filteredData);
});

// Helper function to aggregate data
const aggregateData = (data, groupBy) => {
    const aggregated = data.reduce((acc, curr) => {
        let key;
        const year = curr.Date.getFullYear();
        if (groupBy === 'weekly') {
            const week = getWeek(curr.Date);
            key = `${year}-W${week}`;
        } else if (groupBy === 'monthly') {
            const month = getMonth(curr.Date) + 1; // getMonth is 0-indexed
            key = `${year}-M${month}`;
        } else if (groupBy === 'quarterly') {
            const quarter = getQuarter(curr.Date);
            key = `${year}-Q${quarter}`;
        }

        if (!acc[key]) {
            acc[key] = { demand: 0, count: 0 };
        }
        acc[key].demand += curr.Forecasted_Demand;
        acc[key].count += 1;
        return acc;
    }, {});

    return Object.entries(aggregated).map(([key, value]) => ({
        period: key,
        total_demand: value.demand,
        average_demand: value.demand / value.count
    }));
};

// API endpoints for aggregated data
app.get('/api/forecasts/weekly', (req, res) => {
    const filteredData = filterData(req);
    res.json(aggregateData(filteredData, 'weekly'));
});

app.get('/api/forecasts/monthly', (req, res) => {
    const filteredData = filterData(req);
    res.json(aggregateData(filteredData, 'monthly'));
});

app.get('/api/forecasts/quarterly', (req, res) => {
    const filteredData = filterData(req);
    res.json(aggregateData(filteredData, 'quarterly'));
});

// Function to read and parse the insights CSV data
const loadInsightsData = () => {
    return new Promise((resolve, reject) => {
        const data = [];
        fs.createReadStream(insightsDataPath)
            .pipe(csv())
            .on('data', (row) => {
                data.push(row);
            })
            .on('end', () => {
                insightsData = data;
                console.log('Insights CSV data successfully loaded.');
                resolve(insightsData);
            })
            .on('error', (error) => {
                console.error('Error loading insights CSV data:', error);
                reject(error);
            });
    });
};

// Helper function to filter insights data
const filterInsightsData = (req) => {
    const { Month, PRODUCT_CARD_ID, PRODUCT_NAME } = req.query;
    let filteredData = [...insightsData];

    if (Month) {
        filteredData = filteredData.filter(d => d.Month === Month);
    }
    if (PRODUCT_CARD_ID) {
        filteredData = filteredData.filter(d => d.PRODUCT_CARD_ID === PRODUCT_CARD_ID);
    }
    if (PRODUCT_NAME) {
        filteredData = filteredData.filter(d => d.PRODUCT_NAME === PRODUCT_NAME);
    }
    return filteredData;
};

// API endpoint for AI insights
app.get('/api/insights', (req, res) => {
    const filteredData = filterInsightsData(req);
    res.json(filteredData);
});

// API endpoint to get unique products for dropdowns
app.get('/api/products', (req, res) => {
    const products = [...new Map(forecastData.map(item => [item['PRODUCT_CARD_ID'], {PRODUCT_CARD_ID: item.PRODUCT_CARD_ID, PRODUCT_NAME: item.PRODUCT_NAME}])).values()];
    res.json(products);
});

// ─── SALES APIs ────────────────────────────────────────

// Load sales data
let salesData = [];
const loadSalesData = () => {
    return new Promise((resolve, reject) => {
        const salesPath = path.join(__dirname, 'Data', 'Pump_Data.csv');
        const data = [];
        fs.createReadStream(salesPath)
            .pipe(csv())
            .on('data', (row) => {
                data.push(row);
            })
            .on('end', () => {
                salesData = data;
                console.log('Sales data successfully loaded.');
                resolve(salesData);
            })
            .on('error', (error) => {
                console.error('Error loading sales data:', error);
                reject(error);
            });
    });
};

// Sales KPIs endpoint
app.get('/api/sales/kpis', (req, res) => {
    if (!salesData.length) {
        return res.json({ error: "Sales data not found" });
    }
    try {
        const totalOrders = new Set(salesData.map(item => item['Order Item Id'])).size;
        const totalSales = salesData.reduce((sum, item) => sum + parseFloat(item.Sales || 0), 0);
        const avgDiscount = salesData.reduce((sum, item) => sum + parseFloat(item['Order Item Discount Rate'] || 0), 0) / salesData.length;
        const lateDeliveries = salesData.filter(item => item['Late_delivery_risk'] === '1').length;
        
        res.json({
            total_orders: totalOrders,
            total_sales: Math.round(totalSales * 100) / 100,
            avg_discount: Math.round(avgDiscount * 100 * 100) / 100,
            late_deliveries: lateDeliveries
        });
    } catch (error) {
        res.json({ error: error.message });
    }
});

// Sales metrics endpoint
app.get('/api/sales/:metric', (req, res) => {
    const { metric } = req.params;
    if (!salesData.length) {
        return res.json({ error: "Data not loaded" });
    }

    try {
        if (metric === 'city-sales') {
            const citySales = {};
            salesData.forEach(item => {
                const city = item['Customer City'];
                const sales = parseFloat(item.Sales || 0);
                citySales[city] = (citySales[city] || 0) + sales;
            });
            const sortedCities = Object.entries(citySales)
                .sort(([,a], [,b]) => b - a)
                .slice(0, 10);
            res.json({
                cities: sortedCities.map(([city]) => city),
                sales: sortedCities.map(([,sales]) => sales)
            });
        } else if (metric === 'category-distribution') {
            const categorySales = {};
            salesData.forEach(item => {
                const category = item['Category Name'];
                const sales = parseFloat(item.Sales || 0);
                categorySales[category] = (categorySales[category] || 0) + sales;
            });
            res.json({
                categories: Object.keys(categorySales),
                sales: Object.values(categorySales)
            });
        } else if (metric === 'monthly-sales') {
            const monthlySales = {};
            salesData.forEach(item => {
                const date = new Date(item['order date (DateOrders)']);
                const month = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
                const sales = parseFloat(item.Sales || 0);
                monthlySales[month] = (monthlySales[month] || 0) + sales;
            });
            const sortedMonths = Object.entries(monthlySales).sort();
            res.json({
                months: sortedMonths.map(([month]) => month),
                sales: sortedMonths.map(([,sales]) => sales)
            });
        } else if (metric === 'shipping-mode') {
            const shippingModes = {};
            salesData.forEach(item => {
                const mode = item['Shipping Mode'];
                shippingModes[mode] = (shippingModes[mode] || 0) + 1;
            });
            res.json({
                modes: Object.keys(shippingModes),
                counts: Object.values(shippingModes)
            });
        } else if (metric === 'region-sales') {
            const regionSales = {};
            salesData.forEach(item => {
                const region = item['Order Region'];
                const sales = parseFloat(item.Sales || 0);
                regionSales[region] = (regionSales[region] || 0) + sales;
            });
            const sortedRegions = Object.entries(regionSales).sort(([,a], [,b]) => b - a);
            res.json({
                regions: sortedRegions.map(([region]) => region),
                sales: sortedRegions.map(([,sales]) => sales)
            });
        } else if (metric === 'top-products') {
            const productSales = {};
            salesData.forEach(item => {
                const product = item['Product Name'];
                const sales = parseFloat(item.Sales || 0);
                productSales[product] = (productSales[product] || 0) + sales;
            });
            const sortedProducts = Object.entries(productSales)
                .sort(([,a], [,b]) => b - a)
                .slice(0, 5);
            res.json({
                products: sortedProducts.map(([product]) => product),
                sales: sortedProducts.map(([,sales]) => sales)
            });
        } else {
            res.status(400).json({ error: `Invalid metric '${metric}'` });
        }
    } catch (error) {
        res.json({ error: error.message });
    }
});

// ─── INVENTORY APIs ──────────────────────────────────

// Load inventory data
let stockData = [];
let alertData = [];
let scheduleData = [];
let bomData = [];
let mrpData = [];
let productionOrdersData = [];
let stationScheduleData = [];

const loadInventoryData = () => {
    return Promise.all([
        new Promise((resolve, reject) => {
            const stockPath = path.join(__dirname, 'Data', 'total_stock_levels_updated.csv');
            const data = [];
            fs.createReadStream(stockPath)
                .pipe(csv())
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', () => {
                    stockData = data;
                    resolve();
                })
                .on('error', reject);
        }),
        new Promise((resolve, reject) => {
            const alertPath = path.join(__dirname, 'Data', 'total_demo_sku_inventory_alerts.csv');
            const data = [];
            fs.createReadStream(alertPath)
                .pipe(csv())
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', () => {
                    alertData = data;
                    resolve();
                })
                .on('error', reject);
        }),
        new Promise((resolve, reject) => {
            const schedulePath = path.join(__dirname, 'Data', 'total_production_schedule.csv');
            const data = [];
            fs.createReadStream(schedulePath)
                .pipe(csv())
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', () => {
                    scheduleData = data;
                    resolve();
                })
                .on('error', reject);
        }),
        new Promise((resolve, reject) => {
            const bomPath = path.join(__dirname, 'Data', 'bom_data.csv');
            const data = [];
            fs.createReadStream(bomPath)
                .pipe(csv())
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', () => {
                    bomData = data;
                    resolve();
                })
                .on('error', reject);
        }),
        new Promise((resolve, reject) => {
            const mrpPath = path.join(__dirname, 'Data', 'total_mrp_plan_updated.csv');
            const data = [];
            fs.createReadStream(mrpPath)
                .pipe(csv())
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', () => {
                    mrpData = data;
                    resolve();
                })
                .on('error', reject);
        }),
        new Promise((resolve, reject) => {
            const ordersPath = path.join(__dirname, 'Data', 'total_production_orders.csv');
            const data = [];
            fs.createReadStream(ordersPath)
                .pipe(csv())
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', () => {
                    productionOrdersData = data;
                    resolve();
                })
                .on('error', reject);
        }),
        new Promise((resolve, reject) => {
            const stationPath = path.join(__dirname, 'Data', 'total_station_schedule_updated.csv');
            const data = [];
            fs.createReadStream(stationPath)
                .pipe(csv())
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', () => {
                    stationScheduleData = data;
                    resolve();
                })
                .on('error', reject);
        })
    ]);
};

// Inventory KPIs endpoint
app.get('/api/inventory/kpis', (req, res) => {
    try {
        const totalSkus = new Set(stockData.map(item => item.SKU_No)).size;
        const totalStockOnHand = stockData.reduce((sum, item) => sum + parseInt(item.Stock_On_Hand || 0), 0);
        const inTransit = stockData.reduce((sum, item) => sum + parseInt(item.In_Transit || 0), 0);
        const belowReorderPoint = alertData.filter(item => 
            parseInt(item.Available || 0) < parseInt(item.Reorder_Point || 0)
        ).length;
        const avgLeadTime = stockData.reduce((sum, item) => sum + parseFloat(item.Lead_Time_Days || 0), 0) / stockData.length;
        const scheduledQty = scheduleData.reduce((sum, item) => sum + parseInt(item.Scheduled_Quantity || 0), 0);
        
        res.json({
            total_skus: totalSkus,
            total_stock_on_hand: totalStockOnHand,
            in_transit: inTransit,
            below_reorder_point: belowReorderPoint,
            avg_lead_time: Math.round(avgLeadTime * 100) / 100,
            scheduled_qty: scheduledQty
        });
    } catch (error) {
        res.json({ error: error.message });
    }
});

// Reorder chart endpoint
app.get('/api/inventory/reorder_chart', (req, res) => {
    try {
        const chartData = alertData.map(item => ({
            SKU_No: item.SKU_No,
            Available: parseInt(item.Available || 0),
            Reorder_Point: parseInt(item.Reorder_Point || 0)
        }));
        res.json(chartData);
    } catch (error) {
        res.json({ error: error.message });
    }
});

// Lead times endpoint
app.get('/api/inventory/lead_times', (req, res) => {
    try {
        const leadTimes = stockData.map(item => ({
            SKU_No: item.SKU_No,
            Lead_Time_Days: parseFloat(item.Lead_Time_Days || 0)
        }));
        res.json(leadTimes);
    } catch (error) {
        res.json({ error: error.message });
    }
});

// Supplier alerts endpoint
app.get('/api/inventory/suppliers', (req, res) => {
    try {
        const filtered = alertData.filter(item => 
            parseInt(item.Available || 0) < parseInt(item.Reorder_Point || 0)
        );
        const supplierCounts = {};
        filtered.forEach(item => {
            const supplier = item.Supplier;
            supplierCounts[supplier] = (supplierCounts[supplier] || 0) + 1;
        });
        const result = Object.entries(supplierCounts).map(([supplier, count]) => ({
            Supplier: supplier,
            Alert_Count: count
        }));
        res.json(result);
    } catch (error) {
        res.json({ error: error.message });
    }
});

// Inventory data endpoint
app.get('/api/inventory/:dataset', (req, res) => {
    const { dataset } = req.params;
    const fileMap = {
        'stock_levels': stockData,
        'alerts': alertData,
        'bom': bomData,
        'mrp_plan': mrpData,
        'production_orders': productionOrdersData,
        'schedule': scheduleData,
        'station_schedule': stationScheduleData
    };
    
    if (!fileMap[dataset]) {
        return res.status(400).json({ error: `Invalid dataset '${dataset}'` });
    }
    
    res.json(fileMap[dataset]);
});

// ─── PROCUREMENT APIs ────────────────────────────────

// Load procurement data
let procurementData = [];

const loadProcurementData = () => {
    return new Promise((resolve, reject) => {
        const procurementPath = path.join(__dirname, 'Data', 'smart_procurement_insights_dec2017.csv');
        const data = [];
        fs.createReadStream(procurementPath)
            .pipe(csv())
            .on('data', (row) => {
                data.push(row);
            })
            .on('end', () => {
                procurementData = data;
                console.log('Procurement data successfully loaded.');
                resolve();
            })
            .on('error', reject);
    });
};

// All procurement insights endpoint
app.get('/api/procurement/insights', (req, res) => {
    res.json(procurementData);
});

// Procurement insight by SKU endpoint
app.get('/api/procurement/insight/:sku_id', (req, res) => {
    const { sku_id } = req.params;
    const match = procurementData.filter(item => item.SKU_ID === sku_id);
    if (match.length === 0) {
        return res.status(404).json({ message: `No insight found for SKU ${sku_id}` });
    }
    res.json(match);
});

// ─── OPERATOR, SCHEDULE & SUPPLIER SERVICES ──────────

// Load operator data
let attendanceData = [];
let leaveRequestsData = [];
let operatorInsightsData = [];

const loadOperatorData = () => {
    return Promise.all([
        new Promise((resolve, reject) => {
            const attendancePath = path.join(__dirname, 'Data', 'attendance_log.csv');
            const data = [];
            fs.createReadStream(attendancePath)
                .pipe(csv())
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', () => {
                    attendanceData = data;
                    resolve();
                })
                .on('error', reject);
        }),
        new Promise((resolve, reject) => {
            const janPath = path.join(__dirname, 'Data', 'leave_requests_january.csv');
            const febPath = path.join(__dirname, 'Data', 'leave_requests_february.csv');
            const data = [];
            
            Promise.all([
                new Promise((resolveJan, rejectJan) => {
                    fs.createReadStream(janPath)
                        .pipe(csv())
                        .on('data', (row) => data.push(row))
                        .on('end', resolveJan)
                        .on('error', rejectJan);
                }),
                new Promise((resolveFeb, rejectFeb) => {
                    fs.createReadStream(febPath)
                        .pipe(csv())
                        .on('data', (row) => data.push(row))
                        .on('end', resolveFeb)
                        .on('error', rejectFeb);
                })
            ]).then(() => {
                leaveRequestsData = data;
                resolve();
            }).catch(reject);
        }),
        new Promise((resolve, reject) => {
            const insightsPath = path.join(__dirname, 'Data', 'groq_operator_jan_feb_insights.csv');
            const data = [];
            fs.createReadStream(insightsPath)
                .pipe(csv())
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', () => {
                    operatorInsightsData = data;
                    resolve();
                })
                .on('error', reject);
        })
    ]);
};

// Production KPIs endpoint
app.get('/api/kpis', (req, res) => {
    try {
        const totalOperators = new Set(attendanceData.map(item => item.Operator_ID)).size;
        const today = '2018-01-01';
        const absentToday = attendanceData.filter(item => 
            item.Date === today && item.Present === 'No'
        ).length;
        const totalUnitsScheduled = stationScheduleData.filter(item => 
            item.scheduled_date === '01-01-2018'
        ).reduce((sum, item) => sum + parseInt(item.unit || 0), 0);
        const uniqueProducts = new Set(stationScheduleData.map(item => item.product_name)).size;
        
        res.json({
            total_operators: totalOperators,
            absent_today: absentToday,
            total_units_scheduled: totalUnitsScheduled,
            unique_products: uniqueProducts
        });
    } catch (error) {
        res.status(500).json({ error: `KPI error: ${error.message}` });
    }
});

// Schedule table endpoint
app.get('/api/schedule', (req, res) => {
    if (!stationScheduleData.length) {
        return res.status(500).json({ error: "Schedule file not found or corrupted" });
    }
    
    const formattedData = stationScheduleData.map(item => ({
        Start_Time: item.time,
        Station_Name: item.station,
        Operator_Name: item.operator,
        Model: item.product_model,
        Product: item.product_name,
        Date: item.scheduled_date,
        PO: item.po_number,
        Units: item.unit
    }));
    
    res.json(formattedData);
});

// Station chart data endpoint
app.get('/api/schedule/chart', (req, res) => {
    if (!stationScheduleData.length) {
        return res.status(500).json({ error: "Chart data failed" });
    }
    
    const stationTotals = {};
    stationScheduleData.forEach(item => {
        const station = item.station;
        const units = parseInt(item.unit || 0);
        stationTotals[station] = (stationTotals[station] || 0) + units;
    });
    
    const chartData = Object.entries(stationTotals).map(([station, total]) => ({
        station: station,
        Total_Units: total
    }));
    
    res.json(chartData);
});

// Operator workload endpoint
app.get('/api/schedule/operator_workload', (req, res) => {
    if (!stationScheduleData.length) {
        return res.status(500).json({ error: "Workload data failed" });
    }
    
    const operatorTotals = {};
    stationScheduleData.forEach(item => {
        const operator = item.operator;
        const units = parseInt(item.unit || 0);
        operatorTotals[operator] = (operatorTotals[operator] || 0) + units;
    });
    
    const workloadData = Object.entries(operatorTotals).map(([operator, total]) => ({
        operator: operator,
        Total_Units: total
    }));
    
    res.json(workloadData);
});

// Attendance table endpoint
app.get('/api/attendance', (req, res) => {
    const formattedData = attendanceData.map(item => ({
        date: item.Date,
        operator_id: item.Operator_ID,
        operator_name: item.Operator_Name,
        present: item.Present,
        shift: item.Shift || 'Day'
    })).sort((a, b) => new Date(a.date) - new Date(b.date));
    
    res.json(formattedData);
});

app.get('/api/attendance/table', (req, res) => {
    const formattedData = attendanceData.map(item => ({
        date: item.Date,
        operator_id: item.Operator_ID,
        operator_name: item.Operator_Name,
        present: item.Present,
        shift: item.Shift || 'Day'
    })).sort((a, b) => new Date(a.date) - new Date(b.date));
    
    res.json(formattedData);
});

// Leave requests endpoint
app.get('/api/leaves', (req, res) => {
    const formattedData = leaveRequestsData.map(item => ({
        from_date: item.from_date,
        to_date: item.to_date,
        operator_id: item.operator_id,
        operator_name: item.operator_name,
        reason: item.reason,
        status: item.status
    }));
    
    res.json(formattedData);
});

// All operator insights endpoint
app.get('/api/insights', (req, res) => {
    const filteredInsights = operatorInsightsData.filter(item => 
        item.operator_id && item.ai_insight
    );
    res.json({ insights: filteredInsights });
});

// Insight by operator endpoint
app.get('/api/insights1/:operator_id', (req, res) => {
    const { operator_id } = req.params;
    const filtered = operatorInsightsData.filter(item => item.operator_id === operator_id);
    if (filtered.length === 0) {
        return res.json({ message: `No insights found for ${operator_id}` });
    }
    res.json({ insights1: filtered });
});

// Operator dropdown endpoint
app.get('/api/operators/dropdown', (req, res) => {
    const operators = [...new Set(operatorInsightsData
        .filter(item => item.operator_id)
        .map(item => item.operator_id)
    )].sort();
    res.json({ operators: operators });
});

// ─── SUPPLIER PERFORMANCE APIs ───────────────────────

// Load supplier data
let suppliersData = [];
let alternateSuppliersData = [];
let supplierInsightsData = [];

const loadSupplierData = () => {
    return Promise.all([
        new Promise((resolve, reject) => {
            const suppliersPath = path.join(__dirname, 'Data', 'suppliers.csv');
            const data = [];
            fs.createReadStream(suppliersPath)
                .pipe(csv())
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', () => {
                    suppliersData = data;
                    resolve();
                })
                .on('error', reject);
        }),
        new Promise((resolve, reject) => {
            const altPath = path.join(__dirname, 'Data', 'alternate_suppliers.csv');
            const data = [];
            fs.createReadStream(altPath)
                .pipe(csv())
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', () => {
                    alternateSuppliersData = data;
                    resolve();
                })
                .on('error', reject);
        }),
        new Promise((resolve, reject) => {
            const insightsPath = path.join(__dirname, 'Data', 'ai_supplier_insight_output.csv');
            const data = [];
            fs.createReadStream(insightsPath)
                .pipe(csv())
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', () => {
                    supplierInsightsData = data;
                    resolve();
                })
                .on('error', reject);
        })
    ]);
};

// Supplier data endpoint
app.get('/api/suppliers/:endpoint/:supplier', (req, res) => {
    const { endpoint, supplier } = req.params;
    const match = suppliersData.filter(item => 
        item.Supplier_Name.toLowerCase() === supplier.toLowerCase()
    );
    
    if (match.length === 0) {
        return res.status(404).json({ error: `${supplier} not found` });
    }
    
    const supplierData = match[0];
    
    if (endpoint === 'kpis') {
        res.json({
            supplier: supplierData.Supplier_Name,
            lead_time_days: parseFloat(supplierData.Lead_Time_Days),
            fulfillment_rate_percent: parseFloat(supplierData.Fulfillment_Rate.replace('%', '')),
            otd_percent: parseFloat(supplierData.OTD_Percentage.replace('%', '')),
            late_deliveries: parseInt(supplierData.Late_Deliveries),
            total_orders: parseInt(supplierData.Total_Orders)
        });
    } else if (endpoint === 'metrics') {
        res.json({
            "OTD %": parseFloat(supplierData.OTD_Percentage.replace('%', '')),
            "Quality Score": parseFloat(supplierData.Quality_Score),
            "Fulfillment %": parseFloat(supplierData.Fulfillment_Rate.replace('%', ''))
        });
    } else if (endpoint === 'delivery-stats') {
        const totalOrders = parseInt(supplierData.Total_Orders);
        const lateDeliveries = parseInt(supplierData.Late_Deliveries);
        res.json({
            on_time: totalOrders - lateDeliveries,
            late: lateDeliveries,
            total: totalOrders
        });
    } else {
        res.status(400).json({ error: `Unknown supplier endpoint '${endpoint}'` });
    }
});

// List suppliers endpoint
app.get('/api/suppliers/list', (req, res) => {
    const suppliers = [...new Set(suppliersData
        .filter(item => item.Supplier_Name)
        .map(item => item.Supplier_Name)
    )].sort();
    res.json(suppliers);
});

// Alternate suppliers endpoint
app.get('/api/suppliers/alternates/:supplier', (req, res) => {
    const { supplier } = req.params;
    const mainSupplier = suppliersData.find(item => 
        item.Supplier_Name.toLowerCase() === supplier.toLowerCase()
    );
    
    if (!mainSupplier) {
        return res.status(404).json({ error: `${supplier} not found` });
    }
    
    const skus = mainSupplier.SKU_No;
    const filtered = alternateSuppliersData.filter(item => item.sku_id === skus);
    
    const result = filtered.map(item => ({
        sku_id: item.sku_id,
        supplier_name: item.supplier_name,
        otd_percentage: item.otd_percentage,
        quality_score: item.quality_score,
        email: item.email,
        location: item.location
    })).filter(item => item.sku_id && item.supplier_name);
    
    res.json(result);
});

// Supplier insight endpoint - handle both SKU and supplier name
app.get('/api/suppliers/insight/:identifier', (req, res) => {
    const { identifier } = req.params;
    
    // First try to find by supplier name
    const supplierMatch = suppliersData.find(item => 
        item.Supplier_Name.toLowerCase() === identifier.toLowerCase()
    );
    
    if (supplierMatch) {
        // If found by supplier name, look for insights by SKU
        const skuInsights = supplierInsightsData.filter(item => 
            item.sku_id.toLowerCase() === supplierMatch.SKU_No.toLowerCase()
        );
        
        if (skuInsights.length > 0) {
            return res.json({
                sku_id: supplierMatch.SKU_No,
                supplier_name: supplierMatch.Supplier_Name,
                insight: skuInsights[0].ai_supplier_insight
            });
        }
    }
    
    // If not found by supplier name, try direct SKU lookup
    const skuMatch = supplierInsightsData.filter(item => 
        item.sku_id.toLowerCase() === identifier.toLowerCase()
    );
    
    if (skuMatch.length > 0) {
        return res.json({
            sku_id: identifier,
            insight: skuMatch[0].ai_supplier_insight
        });
    }
    
    return res.status(404).json({ error: `No AI insight found for ${identifier}` });
});

// ─── INSIGHTS APIs ───────────────────────────────────

// Load insights data
let monthlyInsightsData = [];
let quarterlyInsightsData = [];
let yearlyInsightsData = [];

const loadHistoricalInsightsData = () => {
    return Promise.all([
        new Promise((resolve, reject) => {
            const monthlyPath = path.join(__dirname, 'Data', 'groq_monthly_insights.csv');
            const data = [];
            fs.createReadStream(monthlyPath)
                .pipe(csv())
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', () => {
                    monthlyInsightsData = data;
                    resolve();
                })
                .on('error', reject);
        }),
        new Promise((resolve, reject) => {
            const quarterlyPath = path.join(__dirname, 'Data', 'groq_quarterly_regional_insights.csv');
            const data = [];
            fs.createReadStream(quarterlyPath)
                .pipe(csv())
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', () => {
                    quarterlyInsightsData = data;
                    resolve();
                })
                .on('error', reject);
        }),
        new Promise((resolve, reject) => {
            const yearlyPath = path.join(__dirname, 'Data', 'groq_yearly_regional_insights.csv');
            const data = [];
            fs.createReadStream(yearlyPath)
                .pipe(csv())
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', () => {
                    yearlyInsightsData = data;
                    resolve();
                })
                .on('error', reject);
        })
    ]);
};

// Insights by period endpoint
app.get('/api/insights/:period', (req, res) => {
    const { period } = req.params;
    const fileMap = {
        'monthly': monthlyInsightsData,
        'quarterly': quarterlyInsightsData,
        'yearly': yearlyInsightsData
    };
    
    if (!fileMap[period]) {
        return res.status(400).json({ error: `Invalid period '${period}'` });
    }
    
    res.json(fileMap[period]);
});

// ─── HEALTH CHECK ─────────────────────────────────────

app.get('/', (req, res) => {
    res.json({ message: "✅ Unified SCM Express API is up and running!" });
});

// Catch-all middleware for 404s
app.use((req, res, next) => {
    res.status(404).send(`Sorry, can't find that! The requested URL was: ${req.originalUrl}`);
    console.log(`No route found for ${req.method} ${req.originalUrl}`);
});

// Start the server after loading the data
app.listen(port, async () => {
    try {
        await Promise.all([
            loadData(), 
            loadInsightsData(),
            loadSalesData(),
            loadInventoryData(),
            loadProcurementData(),
            loadOperatorData(),
            loadSupplierData(),
            loadHistoricalInsightsData()
        ]);
        console.log(`Server listening at http://localhost:${port}`);
    } catch (error) {
        console.error('Failed to start server:', error);
        process.exit(1);
    }
});
