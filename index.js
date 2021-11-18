var Promise = require('bluebird');
const fs = require('fs');
const csv = require('csv-parser');
const fastcsv = require('fast-csv');
const XLSX = require('xlsx');
const iconv = require('iconv-lite');

const readMain = function() {
  const workbook = XLSX.readFile('main.xlsx');
  const sheet_name_list = workbook.SheetNames;
  let baseTable = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
  return baseTable;
};

const readprice = async function () {
  return new Promise(function(resolve, reject) {
    var lines = [];
    fs
			.createReadStream("/prices/kc/priceukraine.csv")
      .pipe(iconv.decodeStream('win1251'))
      .pipe(csv({ separator: '\t' }))
      .on('data', function (row) {
        if (row['На складе'] != "0")
    				lines.push({
              part_number: row['Артикул'],
              name: row['Наименование (Русский)'],
              priceRtl: row['Старая цена'],
              priceOpt: row['Цена'],
              warranty: row['Гарантия'],
              instock: row['На складе'],
            });
			})
      .on('end', function() {
        resolve(lines);
      });
  });
};

const readBrain = function() {
  const RATEBRAIN = 26.5;
  const workbook = XLSX.readFile('/prices/brain/brain.xlsx');
  const sheet_name_list = workbook.SheetNames;
  let baseTable = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
  let out = baseTable.map(function(product) {
      return {
        part_number: product.Article,
        name: product.Name,
        warranty: product.Warranty,
        instock: 1, // product.Stock + product.DayDelivery,
        priceOpt: product.PriceUSD * RATEBRAIN,
        priceRtl: product.RecommendedPrice,
      };
    });
  return out;
};

const readMezh = function() {
  const workbook = XLSX.readFile('/prices/межигорская/mezhigorska.xlsx');
  const sheet_name_list = workbook.SheetNames;
  let baseTable = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
  let out = baseTable.map(function(product) {
      return {
        part_number: product.part_number,
        name: product.name,
        warranty: product.warranty,
        instock: product.instock,
        priceOpt: product.priceOpt,
      };
    });
  return out;
};

const readPa = function() {
  const workbook = XLSX.readFile('/prices/ember/parced1.xlsm');
  const sheet_name_list = workbook.SheetNames;
  let baseTable = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
  let out = baseTable.map(function(product) {
      return {
        part_number: product.part_number,
        name: product.name,
        warranty: product.warranty,
        instock: product.instock,
        priceOpt: product.price,
      };
    });
  return out;
};

const readEE = function() {
  const workbook = XLSX.readFile('/prices/ember/тов.xlsx');
  const sheet_name_list = workbook.SheetNames;
  let baseTable = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
  let out = baseTable.map(function(product) {
      return {
        part_number: product.part_number,
        name: product.name,
        warranty: product.warranty,
        instock: product.priceRtl,
        priceOpt: product.priceOpt,
        priceRtl: product.priceRtl,
      };
    });
  return out;
};

const readRi = function() {
  const workbook = XLSX.readFile('/prices/рижская/рижская_new1.xlsm');
  const sheet_name_list = workbook.SheetNames;
  let baseTable1 = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
  let baseTable2 = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[1]]);
  let baseTable = baseTable1.concat(baseTable2);
  let mapped = baseTable.map(function(product) {
      return {
        part_number: product.part_number,
        name: product.name,
        warranty: product.warranty,
        instock: product.instock,
        priceOpt: product.priceOpt,
      };
  });
  let out = mapped.filter(function(product) {
    return (product.instock && product.part_number && product.priceOpt);
  });

  return out;
};

const readCh = function() {
  const workbook = XLSX.readFile('/prices/cherg/cherg5.xlsm');
  const sheet_name_list = workbook.SheetNames;
  let baseTable = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
  let mapped = baseTable.map(function(product) {
      return {
        part_number: product.__EMPTY_2,
        name: product.__EMPTY_1,
        warranty: product.warranty,
        instock: product.__EMPTY_4,
        priceOpt: product.__EMPTY_3,
      };
    });
  let out = mapped.filter(function(product) {
        return product.instock;
      });
  //    console.log("3tablews chg length:", baseTable.length,"====");
  //    console.log("3tablews2 chg length:", out.length,"====");
  return out;
};

const readYu = async function() {
  return new Promise(function(resolve, reject) {
    var lines = [];
    fs
			.createReadStream("/PRICES/YUGTORG/PRODUCTS.CSV")
      .pipe(csv({ separator: ';' }))
      .on('data', function (row) {
        if (row[' наличие'] && row[' модель'])
    				lines.push({
              part_number: row[' модель'],
              name: row[' наименование'],
              priceOpt: (row[' цена']) * 1,
              warranty: row[' гарантия (мес.)'],
              instock: 1
            });
			})
      .on('end', function() {
        resolve(lines);
      });
  });
};

const readLookup = function() {
  const workbook = XLSX.readFile('lookup.xls');
  const sheet_name_list = workbook.SheetNames;
  let baseTable = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
  let out = baseTable.map(function(product) {
      return {
        code: product.code,
        name: product.name,
        part_number1: product.part_number1,
        part_number2: product.part_number2,
        shipper: product.shipper,
      };
    });
  return out;
};

const addPN2 = function(mainT1, lookup) {
  let result =[];
  mainT1.forEach((itemTableMain) => {
    let current = {
      key: "",
      ids: []
    };
    let currentPN = itemTableMain.part_number;
    resultLookup = lookup.filter(function(itemLookupTable) {
      return (currentPN == itemLookupTable.part_number1);
    });
    resultLookup.forEach((itemTableLookup) => {
      current['key'] = currentPN;
      current['ids'] = [];
      if (itemTableLookup && itemTableLookup.part_number2) {
        current['ids'].push (itemTableLookup.part_number2);
      }
    });
    result.push(current);
  });
  return result;
}

const priceCalculate = function(priceIn) {
  return Math.round(priceIn * 1.1 + 30);
}

const finder = function(processedTable, itemTableMain, processedLookup) {

  let result = processedTable.find (element => {
    return (element.part_number == itemTableMain.part_number);
  });

  if (result) {
    return result;
  }
  else {
    let foundInLookup = processedLookup.find((element) =>{
      return element.key == itemTableMain.part_number
    });
    if (foundInLookup && foundInLookup.ids) {
      let arrayPN2 = foundInLookup.ids;
      arrayPN2.forEach((itemPN2) => {
        result = processedTable.find (element => {
          return (element.part_number == itemPN2);
        });
        if (result){
          return result;
        }
      });
    }
    if ( !result ) return {
      priceOpt: 0,
      priceRtl: 0,
      warranty: 0,
      instock: 0,
    }
    else
      return result;
  }
}


const chooseShipper = function(product) {
  let data = [
    {
      priceOpt: 0,
      priceComparison : 0,
      priceRtl: 0,
      supplyname: "-",
      warranty: " ",
      instock: 0
    },
    {
      priceOpt: product.mezhPriceOpt * 1,
      priceComparison : product.mezhPriceOpt * 1.02,
      priceRtl: priceCalculate(product.mezhPriceOpt),
      warranty: product.mezhWarranty,
      supplyname: "МЕ",
      instock: product.mezhInstock,
    },
    {
      priceOpt: product.paPriceOpt * 1,
      priceComparison : product.paPriceOpt * 1.2,
      priceRtl: priceCalculate(product.paPriceOpt),
      warranty: product.paWarranty,
      supplyname: "ПА",
      instock: product.paInstock,
    },
    {
      priceOpt: product.riPriceOpt * 1,
      priceComparison : product.riPriceOpt * 1.035,
      priceRtl: priceCalculate(product.riPriceOpt),
      warranty: product.riWarranty,
      supplyname: "РИ",
      instock: product.riInstock,
    },
    {
      priceOpt: product.chPriceOpt * 1,
      priceComparison : product.chPriceOpt * 1.04,
      priceRtl: priceCalculate(product.chPriceOpt),
      warranty: product.chWarranty,
      supplyname: "ЧЕ",
      instock: product.chInstock,
    },
    {
      priceOpt: product.yuPriceOpt * 1,
      priceComparison : product.yuPriceOpt * 1.02,
      priceRtl: priceCalculate(product.chPriceOpt),
      warranty: product.yuWarranty,
      supplyname: "Ю",
      instock: product.yuInstock,
    },
    {
      priceOpt: Math.round(product.brainPriceOpt * 1),
      priceComparison : product.brainPriceOpt * 1,
      priceRtl: (product.brainPriceRtl * 1 > 120) ? product.brainPriceRtl * 1 : priceCalculate(product.brainPriceOpt),
      warranty: product.brainWarranty,
      supplyname: "Б",
      instock: product.brainInstock,
    },
    {
      priceOpt: product.schPriceOpt * 1,
      priceComparison : product.schPriceOpt * 1,
      priceRtl: priceCalculate(product.schPriceOpt),
      warranty: product.schWarranty,
      supplyname: "ЩУ",
      instock: product.schInstock,
    },
    {
      priceOpt: product.eePriceOpt * 1,
      priceComparison : product.eePriceOpt * 1,
      priceRtl: priceCalculate(product.eePriceOpt),
      warranty: product.eeWarranty,
      supplyname: "ИИ",
      instock: product.eeInstock,
    },
  ];
  const min = data.reduce(function(prev, current) {
    if ( !current.priceOpt )
      return prev;
    else if ( !prev.priceOpt )
      return current;
    else
      return ( prev.priceComparison < current.priceComparison ) ? prev : current;
  });

  let result = {
    stock: min.instock,
    stock_status: (min.instock) ? "instock" : "outofstock",
    regular_price: min.priceRtl,
    weight: (min.priceRtl && min.priceOpt) ? min.priceRtl - min.priceOpt : 0,
    supplyname: min.supplyname,
    warranty: min.warranty,
    visibility: "visible",
    cost: min.priceOpt,
  }
  return result; // chooseShipper
};

const main = async function() {
  let tableMain = await readMain();
  let schTable = await readprice();
  let yuTable = await readYu();
  let brainTable = readBrain();
  let mezhTable = readMezh();
  let paTable = readPa();
  let eeTable = readEE();
  let riTable = readRi();
  let chTable = readCh();
  let lookupTable = readLookup();
  let processedLookup = addPN2(tableMain, lookupTable);
//  console.log(processedLookup);
  tableMain.forEach((itemTableMain, index) => {
    itemTableMain.index = index;
    let resultMezh = finder(mezhTable, itemTableMain, processedLookup);
    itemTableMain.mezhPriceOpt = resultMezh.priceOpt;
    itemTableMain.mezhWarranty = resultMezh.warranty;
    itemTableMain.mezhInstock = resultMezh.instock;
    let resultPa = finder(paTable, itemTableMain, processedLookup);
    itemTableMain.paPriceOpt = resultPa.priceOpt;
    itemTableMain.paWarranty = resultPa.warranty;
    itemTableMain.paInstock = resultPa.instock;
    let resultRi = finder(riTable, itemTableMain, processedLookup);
    itemTableMain.riPriceOpt = resultRi.priceOpt;
    itemTableMain.riWarranty = resultRi.warranty;
    itemTableMain.riInstock = resultRi.instock;
    let resultCh = finder(chTable, itemTableMain, processedLookup);
    itemTableMain.chPriceOpt = resultCh.priceOpt;
    itemTableMain.chWarranty = resultCh.warranty;
    itemTableMain.chInstock = resultCh.instock;
    let resultYu = finder(yuTable, itemTableMain, processedLookup);
    itemTableMain.yuPriceOpt = resultYu.priceOpt;
    itemTableMain.yuWarranty = resultYu.warranty;
    itemTableMain.yuInstock = resultYu.instock;
    let resultBrain = finder(brainTable, itemTableMain, processedLookup);
    itemTableMain.brainPriceOpt = resultBrain.priceOpt;
    itemTableMain.brainPriceRtl = resultBrain.priceRtl;
    itemTableMain.brainWarranty = resultBrain.warranty;
    itemTableMain.brainInstock = resultBrain.instock;
    let resultSch = finder(schTable, itemTableMain, processedLookup);
    itemTableMain.schPriceOpt = resultSch.priceOpt;
    itemTableMain.schPriceRtl = resultSch.priceRtl;
    itemTableMain.schWarranty = resultSch.warranty;
    itemTableMain.schInstock = resultSch.instock;
    let resultEe = finder(eeTable, itemTableMain, processedLookup);
    itemTableMain.eePriceOpt = resultEe.priceOpt;
    itemTableMain.eePriceRtl = resultEe.priceRtl;
    itemTableMain.eeWarranty = resultEe.warranty;
    itemTableMain.eeInstock = resultEe.instock;
  });
  let step1 = tableMain.map(product => {
    let productFromShipper = chooseShipper(product);
    return {
      post_title: product.post_title,
      id: product.id,
      stock: productFromShipper.stock,
      post_name: product.post_name,
      stock_status: productFromShipper.stock_status,
      cost: productFromShipper.cost,
      regular_price: productFromShipper.regular_price,
      weight: productFromShipper.weight,
      supplyname: productFromShipper.supplyname,
      warranty: productFromShipper.warranty,
      visibility: productFromShipper.visibility,
      part_number: product.part_number,
    }
  });
  await writeCSV2(tableMain, "out1.csv");
  await writeCSV2(step1, "step1.csv");
  await writeCSV2(processedLookup, "processedLookup.csv");
  showSummary(step1);

  console.log("finished");
}

main();

const writeCSV2 = async function(lines, filename) {
		const ws = fs.createWriteStream(filename);
	  return new Promise(function(resolve, reject) {
				 fastcsv
				   .write(lines, { headers: true, delimiter: '\t' })
				   .pipe(ws);
				 resolve(lines);
	  });
		resolve();
}


const showSummary = async function(exp) {
  const totalQuantityOld = 1138;
  const totalQuantityNew = exp.filter(product => product.stock_status == "instock").length;
  console.log(`file exp prepared as step1.csv, positions: ${totalQuantityOld}/  ${totalQuantityNew}` );
  console.log("Поставщик РИ: 242/ ", exp.filter(
    product => product.stock_status == "instock" && product.supplyname == "РИ"
  ).length);
  console.log("Поставщик МЕ: 46/ ", exp.filter(
    product => product.stock_status == "instock" && product.supplyname == "МЕ"
  ).length);
  console.log("Поставщик ЩУ: 72/ ", exp.filter(
    product => product.stock_status == "instock" && product.supplyname == "ЩУ"
  ).length);
  console.log("Поставщик ЧЕ: 58/ ", exp.filter(
    product => product.stock_status == "instock" && product.supplyname == "ЧЕ"
  ).length);
  console.log("Поставщик Б: 527/ ", exp.filter(
    product =>
      product.stock_status == "instock" && product.supplyname.includes("Б")
  ).length);
  console.log("Поставщик ПА: 136/ ", exp.filter(
    product =>
      product.stock_status == "instock" && product.supplyname.includes("ПА")
  ).length);
  if ( Math.abs (totalQuantityNew - totalQuantityOld ) > 60 )
    console.log(`Слишком большая разница в количестве товаров, проверьте количества и файл экспорта PLI afterhot.csv!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!` );
  else
    console.log(`Quality control afterhot.csv Ok!` );
};
