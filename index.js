const axios = require("axios");
const ExcelJS = require("exceljs");
const fs = require("fs");
const pg = require("pg");
const { parseString } = require("xml2js");

const staging =
  "postgresql://irf:RQWW4c397xzEVNL2@192.168.0.51:5433/irf?schema=public";
const dev =
  "postgresql://irf:irf@cerestar-irf.zero-one.cloud:5432/irf?schema=public";
const localDb = "postgresql://irf:irf@localhost:5432/irf?schema=public";
const pool = new pg.Pool({
  connectionString: localDb,
});
const navWebServiceUrl = `http://CSV-002.SBI.local:8648/CFM_NEW/ODataV4/Company('CFM-CILEGON')/Item`;

async function dumData() {
  const client = await pool.connect();

  await client.query("BEGIN");
  try {
    const data = [];
    const workbook = new ExcelJS.Workbook();
    const sheets = (await workbook.xlsx.readFile("items.xlsx")).getWorksheet(
      "items-dump"
    );

    sheets.eachRow({ includeEmpty: false }, (row, rowNumber) => {
      if (rowNumber > 1) {
        data.push({
          no: row.values[1],
          description: row.values[2],
          description_two: row.values[3],
          material: row.values[4],
          specification: row.values[5],
          feature: row.values[6],
          manufacturer: row.values[7],
          manufacturer_part_no: row.values[8],
          base_unit_of_measure: row.values[9],
          mro_code: row.values[10],
          event_type: row.values[11],
          status_item: "Active",
          check_item: 1,
        });
      }
    });
    const results = data.filter((d) => d.mro_code !== "");
    console.log(results);
    const query =
      "INSERT INTO event_stream_items (no_nav,description,description_2,material,specification,feature,manufacturer,manufacturer_part_no,base_unit_of_measure,mro_code,event_type,status_item,check_item) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)";
    await Promise.all(
      results.map(async (d) => {
        return await client.query(query, [
          d.no,
          d.description,
          d.description_two,
          d.material,
          d.specification,
          d.feature,
          d.manufacturer,
          d.manufacturer_part_no,
          d.base_unit_of_measure,
          d.mro_code,
          d.event_type,
          d.status,
          d.check_item,
        ]);
      })
    );
    // await Promise.all(
    //   data.map(async (d) => {
    //     return await client.query(`INSERT INTO "items" ("no","full_description","description","description_2","material","specification","feature","manufacturer","manufacturer_part_no","base_unit_of_measure","gen_prod_posting_group","inventory_posting_group","status","check_item")
    //       VALUES ('${d.no}','${d.full_description}','${d.description}','${d.description_two}','${d.material}','${d.specification}','${d.feature}','${d.manufacturer}','${d.manufacturer_part_no}','${d.base_unit_of_measure}','${d.gen_prod_posting_group}','${d.inventory_posting_group}','${d.status}','${d.check_item}')
    //     `);
    //   })
    // );
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    console.log(error);
  } finally {
    client.release();
  }
  await pool.end();
}
async function fetchData() {
  try {
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet("items-dump");
    const response = await axios.get(navWebServiceUrl, {
      auth: {
        username: "it plant",
        password: "Ww12345678",
      },
    });
    const result = response.data.value;
    const propertyName = Object.keys(result[0]);
    const setColumn = propertyName.map((p) => {
      return {
        header: p,
        key: p.toLowerCase(),
        width: 20,
      };
    });
    worksheet.columns = setColumn;
    const data = result.map((r) => {
      const newItem = {};
      propertyName.forEach((p) => {
        if (r.hasOwnProperty(p)) {
          newItem[p.toLowerCase()] = r[p];
        }
      });
      return newItem;
    });

    worksheet.addRows(data);
    const filePath = "item_cfm_cilegon.xlsx";
    workbook.xlsx
      .writeFile(filePath)
      .then(() => {
        console.log("Already export");
      })
      .catch((error) => {
        console.error("Error", error.message);
      });
    //console.log(response.data.value);
    // // Parse the XML response
    // const xmlData = response.data;
    // parseString(xmlData, (err, result) => {
    //   if (err) {
    //     throw err;
    //   }

    //   // Access parsed data
    //   console.log("Parsed XML:", result);

    //   // Process the parsed data as needed
    //   // ...
    // });
  } catch (error) {
    console.error("Error:", error.message);
  }
}

(async () => {
  console.time("START INSERT");
  console.log("==START INSERT DATA==");
  console.log(await dumData());
  console.log("==FINISH INSERT DATA==");
  console.timeEnd("START INSERT");
})();