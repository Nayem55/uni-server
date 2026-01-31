const express = require("express");
const cors = require("cors");
const { MongoClient, ServerApiVersion, ObjectId } = require("mongodb");
const dayjs = require("dayjs");
const { default: axios } = require("axios");

require("dotenv").config();
const port = parseInt(process.env.PORT) || 5000;

const app = express();

app.use(express.json({ limit: "100mb" }));
app.use(cors()); // Allow cross-origin requests

//................Test version................
const mongoURI =
  "mongodb+srv://wh-pos:wh-pos@cluster0.rabgbwv.mongodb.net/?retryWrites=true&w=majority";

const client = new MongoClient(mongoURI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
  serverApi: ServerApiVersion.v1,
});

async function run() {
  try {
    const products = client.db("wh-pos").collection("products");
    const users = client.db("wh-pos").collection("users");
    const salesCollection = client.db("wh-pos").collection("sales");
    const targetsCollection = client.db("wh-pos").collection("targets");
    const tdda = client.db("wh-pos").collection("tdda");
    const orderRequests = client.db("wh-pos").collection("orderRequests");
    const brandTargets = client.db("wh-pos").collection("brandTargets");
    const categoryTargetsCollection = client
      .db("wh-pos")
      .collection("categoryTargets");
    const moneyTransactions = client
      .db("wh-pos")
      .collection("money_transactions");
    const outlet_collection = client
      .db("wh-pos")
      .collection("outlet_collection");
    const stock_transactions = client
      .db("wh-pos")
      .collection("stock_transactions");
    const outletStockCollection = client
      .db("wh-pos")
      .collection("outlet_stock");
    const category = client.db("wh-pos").collection("category");
    const brand = client.db("wh-pos").collection("brand");
    const pricelevel = client.db("wh-pos").collection("pricelevel");

    app.post("/add-outlet-stock", async (req, res) => {
      try {
        const productsCollection = client.db("wh-pos").collection("products");

        // Fetch all products
        const products = await productsCollection.find().toArray();

        // List of outlets
        const outlets = ["AMD Warehouse", "GVI Warehouse", "RL Warehouse"];
        // Prepare bulk insert data with separate opening and current stock
        const stockEntries = products.map((product) => ({
          barcode: product.barcode,
          outlet_stocks: outlets.reduce((acc, outlet) => {
            acc[outlet] = {
              openingStock: 100,
              currentStock: 100,
              openingStockValueDP: 0 * product.dp,
              openingStockValueTP: 0 * product.tp,
              currentStockValueDP: 0 * product.dp,
              currentStockValueTP: 0 * product.tp,
            };
            return acc;
          }, {}),
        }));

        // Insert all stock entries in bulk
        await outletStockCollection.insertMany(stockEntries);

        res
          .status(200)
          .json({ message: "Outlet stock initialized successfully" });
      } catch (error) {
        console.error("Error initializing outlet stock:", error);
        res
          .status(500)
          .json({ message: "Error initializing outlet stock", error });
      }
    });
    app.post("/reset-outlets-due", async (req, res) => {
      try {
        // Reset all outlets' due amounts to 0
        const result = await outlet_collection.updateMany(
          {}, // Empty filter to match all documents
          {
            $set: {
              current_due: 0,
              opening_due: 0,
            },
          },
        );

        return res.status(200).json({
          success: true,
          message: `Successfully reset due amounts for ${result.modifiedCount} outlets`,
          modifiedCount: result.modifiedCount,
        });
      } catch (error) {
        console.error("Error resetting outlets due:", error);
        return res.status(500).json({
          success: false,
          message: "Failed to reset outlets due",
          error: error.message,
        });
      }
    });
    // Improved API endpoint to automatically create outlet stocks
    app.post("/create-product-with-stocks", async (req, res) => {
      try {
        const { productData } = req.body;
        const productsCollection = client.db("wh-pos").collection("products");
        const outletStockCollection = client
          .db("wh-pos")
          .collection("outlet_stock");

        // 1. First create the product
        const productResult = await productsCollection.insertOne(productData);
        const newProduct = { ...productData, _id: productResult.insertedId };

        // 2. Define outlets (could also be fetched from a database)
        const outlets = ["AMD Warehouse", "GVI Warehouse", "RL Warehouse"];

        // 3. Create stock entries for all outlets
        const stockEntry = {
          barcode: newProduct.barcode,
          outlet_stocks: outlets.reduce((acc, outlet) => {
            acc[outlet] = {
              openingStock: 0,
              currentStock: 0,
              openingStockValueDP: 0 * newProduct.dp,
              openingStockValueTP: 0 * newProduct.tp,
              currentStockValueDP: 0 * newProduct.dp,
              currentStockValueTP: 0 * newProduct.tp,
            };
            return acc;
          }, {}),
        };

        // 4. Insert the stock entry
        await outletStockCollection.insertOne(stockEntry);

        res.status(201).json({
          message: "Product and outlet stocks created successfully",
          product: newProduct,
          stockEntry,
        });
      } catch (error) {
        console.error("Error creating product with stocks:", error);
        res.status(500).json({
          message: "Error creating product with stocks",
          error: error.message,
        });
      }
    });

    app.post("/add-new-outlet", async (req, res) => {
      try {
        const {
          name,
          proprietorName,
          address,
          contactNumber,
          nidNumber,
          binNumber,
          tinNumber,
          attachment,
        } = req.body;

        // Validate required fields
        if (
          !name ||
          !proprietorName ||
          !address ||
          !contactNumber ||
          !nidNumber
        ) {
          return res.status(400).json({
            message:
              "Name, proprietor name, address, contact number, and NID number are required",
          });
        }

        // Check if outlet already exists
        const existingOutlet = await outlet_collection.findOne({
          outlet_name: name,
        });
        if (existingOutlet) {
          return res
            .status(400)
            .json({ message: "Outlet with this name already exists" });
        }

        // Get all products
        const allProducts = await outletStockCollection.find().toArray();

        // Prepare bulk update operations for stock initialization
        const bulkUpdateOps = allProducts.map((product) => ({
          updateOne: {
            filter: { barcode: product.barcode },
            update: {
              $set: {
                [`outlet_stocks.${name}`]: {
                  openingStock: 0,
                  currentStock: 0,
                  openingStockValueDP: 0,
                  openingStockValueTP: 0,
                  currentStockValueDP: 0,
                  currentStockValueTP: 0,
                },
              },
            },
          },
        }));

        // Execute bulk update
        const stockUpdateResult = await outletStockCollection.bulkWrite(
          bulkUpdateOps,
        );

        // Create new outlet document
        const newOutlet = {
          outlet_name: name,
          proprietor_name: proprietorName,
          address: address,
          contact_number: contactNumber,
          nid_number: nidNumber,
          bin_number: binNumber || null,
          tin_number: tinNumber || null,
          attachment: attachment || null,
          current_due: 0,
          opening_due: 0,
          created_at: new Date(),
          updated_at: new Date(),
        };

        // Insert into outlets collection
        const outletInsertResult = await outlet_collection.insertOne(newOutlet);

        return res.status(201).json({
          message: `Outlet "${name}" created successfully`,
          data: {
            outlet: newOutlet,
            productsUpdated: stockUpdateResult.modifiedCount,
          },
        });
      } catch (error) {
        console.error("Error creating new outlet:", error);
        return res.status(500).json({
          message: "Error creating new outlet",
          error: error.message,
        });
      }
    });

    app.put("/update-outlet/:outletName", async (req, res) => {
      try {
        const { outletName } = req.params;

        const {
          name, // new outlet name if being changed
          proprietorName,
          address,
          contactNumber,
          nidNumber,
          binNumber,
          tinNumber,
          attachment,
        } = req.body;

        // Check if outlet exists
        const existingOutlet = await outlet_collection.findOne({
          outlet_name: outletName,
        });
        if (!existingOutlet) {
          return res.status(404).json({ message: "Outlet not found" });
        }

        // If name is being changed, check if new name already exists
        if (name !== outletName) {
          const nameExists = await outlet_collection.findOne({
            outlet_name: name,
          });
          if (nameExists) {
            return res.status(400).json({
              message: "Another outlet with this name already exists",
            });
          }
        }

        // Prepare update object
        const updateData = {
          outlet_name: name,
          proprietor_name: proprietorName,
          address: address,
          contact_number: contactNumber,
          nid_number: nidNumber,
          bin_number: binNumber || null,
          tin_number: tinNumber || null,
          attachment: attachment || existingOutlet.attachment, // keep existing if not provided
          updated_at: new Date(),
        };

        // Update outlet document
        const updateResult = await outlet_collection.updateOne(
          { outlet_name: outletName },
          { $set: updateData },
        );

        if (updateResult.modifiedCount === 0) {
          return res.status(200).json({
            message: "No changes were made to the outlet",
            data: existingOutlet,
          });
        }

        // If outlet name was changed, update all references in other collections
        if (name !== outletName) {
          // Update in outlet_stocks collection (only the field name, not stock data)
          await outletStockCollection.updateMany(
            {},
            {
              $rename: {
                [`outlet_stocks.${outletName}`]: `outlet_stocks.${name}`,
              },
            },
          );

          // Update in any other collections that might reference outlet name
          // Example for sales collection:
          // await salesCollection.updateMany(
          //   { outlet: outletName },
          //   { $set: { outlet: name } }
          // );
        }

        // Get updated outlet data
        const updatedOutlet = await outlet_collection.findOne({
          outlet_name: name,
        });

        return res.status(200).json({
          message: `Outlet "${outletName}" updated successfully`,
          data: updatedOutlet,
        });
      } catch (error) {
        console.error("Error updating outlet:", error);
        return res.status(500).json({
          message: "Error updating outlet",
          error: error.message,
        });
      }
    });

    app.post("/transfer-outlet-stock", async (req, res) => {
      try {
        const { sourceOutlet, targetOutlet } = req.body;

        if (!sourceOutlet || !targetOutlet) {
          return res.status(400).json({
            message: "Both source and target outlet names are required",
          });
        }

        if (sourceOutlet === targetOutlet) {
          return res.status(400).json({
            message: "Source and target outlets cannot be the same",
          });
        }

        // Get the due amounts from both outlets
        const sourceOutletData = await outlet_collection.findOne({
          outlet_name: sourceOutlet,
        });

        if (!sourceOutletData) {
          return res.status(404).json({
            message: `Source outlet "${sourceOutlet}" not found`,
          });
        }

        const targetOutletData = await outlet_collection.findOne({
          outlet_name: targetOutlet,
        });

        if (!targetOutletData) {
          return res.status(404).json({
            message: `Target outlet "${targetOutlet}" not found`,
          });
        }

        // Get all products that have stock in the source outlet
        const productsWithSourceStock = await outletStockCollection
          .find({
            [`outlet_stocks.${sourceOutlet}`]: { $exists: true },
          })
          .toArray();

        // Prepare bulk update operations to transfer stock
        const bulkUpdateOps = productsWithSourceStock.map((product) => {
          const sourceStock = product.outlet_stocks[sourceOutlet];

          // Initialize target outlet stock if it doesn't exist
          const targetStock = product.outlet_stocks[targetOutlet] || {
            openingStock: 0,
            currentStock: 0,
            openingStockValueDP: 0,
            openingStockValueTP: 0,
            currentStockValueDP: 0,
            currentStockValueTP: 0,
          };

          return {
            updateOne: {
              filter: { barcode: product.barcode },
              update: {
                $set: {
                  // Update target outlet stock by adding source values
                  [`outlet_stocks.${targetOutlet}`]: {
                    openingStock:
                      targetStock.openingStock + sourceStock.openingStock,
                    currentStock:
                      targetStock.currentStock + sourceStock.currentStock,
                    openingStockValueDP:
                      targetStock.openingStockValueDP +
                      sourceStock.openingStockValueDP,
                    openingStockValueTP:
                      targetStock.openingStockValueTP +
                      sourceStock.openingStockValueTP,
                    currentStockValueDP:
                      targetStock.currentStockValueDP +
                      sourceStock.currentStockValueDP,
                    currentStockValueTP:
                      targetStock.currentStockValueTP +
                      sourceStock.currentStockValueTP,
                  },
                  // Reset source outlet stock to 0 instead of removing it
                  [`outlet_stocks.${sourceOutlet}`]: {
                    openingStock: 0,
                    currentStock: 0,
                    openingStockValueDP: 0,
                    openingStockValueTP: 0,
                    currentStockValueDP: 0,
                    currentStockValueTP: 0,
                  },
                },
              },
            },
          };
        });

        // Execute bulk update for stock transfer
        const stockTransferResult = await outletStockCollection.bulkWrite(
          bulkUpdateOps,
        );

        // Transfer due amounts to target and reset source
        const updatedTargetDue =
          (targetOutletData.current_due || 0) +
          (sourceOutletData.current_due || 0);

        // Update both outlets in a single operation
        await outlet_collection.bulkWrite([
          {
            updateOne: {
              filter: { outlet_name: targetOutlet },
              update: {
                $set: {
                  current_due: updatedTargetDue,
                  opening_due: updatedTargetDue,
                },
              },
            },
          },
          {
            updateOne: {
              filter: { outlet_name: sourceOutlet },
              update: {
                $set: {
                  current_due: 0,
                  // opening_due: 0,
                },
              },
            },
          },
        ]);

        return res.status(200).json({
          message: `Successfully transfered all stock and due from "${sourceOutlet}" to "${targetOutlet}"`,
          productsUpdated: stockTransferResult.modifiedCount,
          amountsTransfered: {
            stock: {
              items: productsWithSourceStock.length,
              currentStock: productsWithSourceStock.reduce(
                (sum, product) =>
                  sum + product.outlet_stocks[sourceOutlet].currentStock,
                0,
              ),
              currentValueDP: productsWithSourceStock.reduce(
                (sum, product) =>
                  sum + product.outlet_stocks[sourceOutlet].currentStockValueDP,
                0,
              ),
              currentValueTP: productsWithSourceStock.reduce(
                (sum, product) =>
                  sum + product.outlet_stocks[sourceOutlet].currentStockValueTP,
                0,
              ),
            },
            due: {
              current_due: sourceOutletData.current_due,
              opening_due: sourceOutletData.opening_due,
            },
          },
        });
      } catch (error) {
        console.error("Error transferring outlet stock:", error);
        return res.status(500).json({
          message: "Error transferring outlet stock",
          error: error.message,
        });
      }
    });

    app.delete("/delete-outlet/:outletName", async (req, res) => {
      try {
        const { outletName } = req.params;

        // 1. Remove the outlet from outlet_stocks collection
        const allProducts = await outletStockCollection.find().toArray();
        const bulkUpdateOps = allProducts.map((product) => ({
          updateOne: {
            filter: { barcode: product.barcode },
            update: {
              $unset: {
                [`outlet_stocks.${outletName}`]: "",
              },
            },
          },
        }));

        await outletStockCollection.bulkWrite(bulkUpdateOps);

        // 2. Delete the outlet from outlets collection
        const deleteResult = await outlet_collection.deleteOne({
          outlet_name: outletName,
        });

        if (deleteResult.deletedCount === 0) {
          return res.status(404).json({
            message: `Outlet "${outletName}" not found`,
          });
        }

        return res.status(200).json({
          message: `Outlet "${outletName}" deleted successfully`,
          productsUpdated: bulkUpdateOps.length,
        });
      } catch (error) {
        console.error("Error deleting outlet:", error);
        return res.status(500).json({
          message: "Error deleting outlet",
          error: error.message,
        });
      }
    });

    app.get("/api/stock-value/:outletName", async (req, res) => {
      try {
        const outletName = decodeURIComponent(req.params.outletName);
        // First get all documents
        const allDocs = await outletStockCollection.find({}).toArray();

        // Manually aggregate the values
        let totals = {
          totalCurrentDP: 0,
          totalCurrentTP: 0,
          totalOpeningDP: 0,
          totalOpeningTP: 0,
        };

        allDocs.forEach((doc) => {
          if (doc.outlet_stocks && doc.outlet_stocks[outletName]) {
            const outletData = doc.outlet_stocks[outletName];
            totals.totalCurrentDP += parseFloat(
              outletData.currentStockValueDP || 0,
            );
            totals.totalCurrentTP += parseFloat(
              outletData.currentStockValueTP || 0,
            );
            totals.totalOpeningDP += parseFloat(
              outletData.openingStockValueDP || 0,
            );
            totals.totalOpeningTP += parseFloat(
              outletData.openingStockValueTP || 0,
            );
          }
        });

        res.status(200).json({
          outlet: outletName,
          ...totals,
        });
      } catch (error) {
        console.error("Error calculating stock value:", error);
        res.status(500).json({
          message: "Error calculating stock value",
          error: error.message,
        });
      }
    });

    app.get("/current-due/:outletName", async (req, res) => {
      try {
        const outletName = decodeURIComponent(req.params.outletName);

        const user = await outlet_collection
          .find({
            outlet_name: { $regex: new RegExp(`^${outletName}$`, "i") },
          })
          .toArray(); // Only if using native MongoDB

        res.status(200).json(user[0]);
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    app.get("/get-outlets", async (req, res) => {
      try {
        const outlets = await outlet_collection
          .find({}, { projection: { outlet_name: 1 } }) // Only fetch outlet_name
          .toArray();

        const outletNames = outlets.map((outlet) => outlet.outlet_name);

        res.status(200).json(outletNames); // Return just the names
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });
    app.get("/get-outlets-full", async (req, res) => {
      try {
        const outlets = await outlet_collection.find({}).toArray();

        res.status(200).json(outlets);
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    app.put("/update-products-promo", async (req, res) => {
      try {
        const result = await products.updateMany(
          {}, // Update all documents
          [
            {
              $set: {
                promoDP: "$dp", // Correct way to copy dp value
                promoTP: "$tp", // Correct way to copy tp value
              },
            },
          ],
        );

        res.json({
          message: "All products updated successfully",
          modifiedCount: result.modifiedCount,
        });
      } catch (error) {
        console.error("Error updating products:", error);
        res.status(500).json({ message: "Error updating products", error });
      }
    });

    // Get all products with only name and barcode
    app.get("/all-products", async (req, res) => {
      try {
        const items = await products
          .find({}, { projection: { name: 1, barcode: 1, _id: 0 } })
          .toArray();

        res.send(items);
      } catch (error) {
        console.error("Error fetching products:", error);
        res.status(500).json({ message: "Error fetching products", error });
      }
    });

    app.get("/all-product", async (req, res) => {
      try {
        const items = await products.find({}).toArray();

        res.send(items);
      } catch (error) {
        console.error("Error fetching products:", error);
        res.status(500).json({ message: "Error fetching products", error });
      }
    });

    // Get all products with pagination
    app.get("/products", async (req, res) => {
      try {
        const page = parseInt(req.query.page) || 1;
        const limit = 20;
        const skip = (page - 1) * limit;

        const items = await products.find().skip(skip).limit(limit).toArray();
        const totalCount = await products.countDocuments();

        res.status(200).json({
          products: items,
          totalPages: Math.ceil(totalCount / limit),
          currentPage: page,
        });
      } catch (error) {
        console.error("Error fetching products:", error);
        res.status(500).json({ message: "Error fetching products", error });
      }
    });

    // Add a new product
    app.post("/products", async (req, res) => {
      try {
        const newProduct = req.body;
        const result = await products.insertOne(newProduct);
        res.status(201).json({ _id: result.insertedId, ...newProduct });
      } catch (error) {
        res.status(500).json({ error: "Failed to add product" });
      }
    });

    // Update a product
    app.put("/products/:id", async (req, res) => {
      try {
        const { id } = req.params;

        if (!ObjectId.isValid(id)) {
          return res.status(400).json({ error: "Invalid product ID" });
        }

        const updatedProduct = { ...req.body };
        delete updatedProduct._id; // Remove _id before updating

        const result = await products.updateOne(
          { _id: new ObjectId(id) },
          { $set: updatedProduct },
        );

        if (result.modifiedCount === 0) {
          return res
            .status(404)
            .json({ error: "Product not found or no changes made" });
        }

        res.json({ message: "Product updated successfully" });
      } catch (error) {
        console.error("Update Error:", error);
        res.status(500).json({ error: "Failed to update product" });
      }
    });
    // Bulk prefix brand & category for products whose *name* contains "SD: "
    app.put("/products/bulk/update-prefix", async (req, res) => {
      try {
        // Match strictly on name containing "SD: " (exact case & space)
        const match = { name: { $regex: /SD:\s/ } };

        // Run as an aggregation-pipeline update (MongoDB 4.2+)
        const result = await products.updateMany(match, [
          {
            $set: {
              brand: {
                $cond: [
                  {
                    $regexMatch: {
                      input: { $ifNull: ["$brand", ""] },
                      regex: /^SD:\s/,
                    },
                  },
                  "$brand",
                  {
                    $cond: [
                      { $gt: [{ $strLenCP: { $ifNull: ["$brand", ""] } }, 0] },
                      { $concat: ["SD: ", "$brand"] },
                      "$brand",
                    ],
                  },
                ],
              },
              category: {
                $cond: [
                  {
                    $regexMatch: {
                      input: { $ifNull: ["$category", ""] },
                      regex: /^SD:\s/,
                    },
                  },
                  "$category",
                  {
                    $cond: [
                      {
                        $gt: [{ $strLenCP: { $ifNull: ["$category", ""] } }, 0],
                      },
                      { $concat: ["SD: ", "$category"] },
                      "$category",
                    ],
                  },
                ],
              },
            },
          },
        ]);

        res.json({
          message:
            'Prefixed "SD: " to brand/category for SD products (matched by name).',
          matchedCount: result.matchedCount,
          modifiedCount: result.modifiedCount,
        });
      } catch (error) {
        console.error("SD bulk prefix error:", error);
        res.status(500).json({ error: "Failed to bulk update brand/category" });
      }
    });

    // Delete a product
    app.delete("/products/:id", async (req, res) => {
      try {
        const { id } = req.params;
        await products.deleteOne({ _id: new ObjectId(id) });
        res.json({ message: "Product deleted successfully" });
      } catch (error) {
        res.status(500).json({ error: "Failed to delete product" });
      }
    });

    app.put("/products/bulk-update-from-excel", async (req, res) => {
      try {
        const { updates, priceListUpdates } = req.body;

        if (!updates || !Array.isArray(updates)) {
          return res.status(400).json({ error: "Invalid update data" });
        }

        // Process main product updates
        const bulkOps = updates.map((update) => ({
          updateOne: {
            filter: { _id: update._id },
            update: { $set: update.updateFields },
          },
        }));

        // Process price list updates
        let updatedPriceLists = 0;
        if (priceListUpdates && Array.isArray(priceListUpdates)) {
          for (const priceUpdate of priceListUpdates) {
            const updateObj = {};
            if (priceUpdate.prices.dp !== undefined)
              updateObj[`priceList.${priceUpdate.outlet}.dp`] =
                priceUpdate.prices.dp;
            if (priceUpdate.prices.tp !== undefined)
              updateObj[`priceList.${priceUpdate.outlet}.tp`] =
                priceUpdate.prices.tp;
            if (priceUpdate.prices.mrp !== undefined)
              updateObj[`priceList.${priceUpdate.outlet}.mrp`] =
                priceUpdate.prices.mrp;

            if (Object.keys(updateObj).length > 0) {
              await products.updateOne(
                { _id: priceUpdate._id },
                { $set: updateObj },
              );
              updatedPriceLists++;
            }
          }
        }

        const result = await products.bulkWrite(bulkOps);

        res.status(200).json({
          message: "Bulk update from Excel completed",
          matchedCount: result.matchedCount,
          modifiedCount: result.modifiedCount,
          updatedCount: result.modifiedCount,
          updatedPriceLists,
        });
      } catch (error) {
        console.error("Error in bulk update from Excel:", error);
        res.status(500).json({ error: "Internal server error" });
      }
    });
    // Search products by name or barcode
    app.get("/search-product", async (req, res) => {
      try {
        const { search, type } = req.query;
        let filter = {};
        if (search) {
          if (type === "barcode") {
            filter = { barcode: search };
          } else {
            filter = { name: new RegExp(search, "i") };
          }
        }

        const items = await products.find(filter).toArray();
        res.status(200).json(items);
      } catch (error) {
        console.error("Error searching products:", error);
        res.status(500).json({ message: "Error searching products", error });
      }
    });

    // 🔹 Bulk Insert API
    app.post("/categories/bulk-insert", async (req, res) => {
      const categoryArray = [
        "Air Freshener 300ML: Lattafa",
        "Almond Oil: RS",
        "Apple Cider Vinegar: RS",
        "Armaf",
        "Armaf Enchanted",
        "Armaf Magnum",
        "Baby Range: Clariss",
        "Body Milk: Earth Beauty & You",
        "Body Spray 150ML: Assorted",
        "Castor Oil: RS",
        "Colour Me",
        "Deodrants: Clariss",
        "Euroliva, S.A.",
        "Extra Virgin Olive Oil: RS",
        "Face Wash: Clariss",
        "Face Wash: Earth Beauty & You",
        "Food: Clariss",
        "Hand Wash: Earth Beauty & You",
        "Layer'r Shot",
        "Layer'r Shot Absolute",
        "Layer'r Shot Deodorant",
        "Layer'r Shot Perfume",
        "Layer'r Shot Pocket",
        "Layer'r Wottagirl",
        "Layer'r Wottagirl Perfume",
        "Mist Toner: Earth Beauty & You",
        "Moisturizer Cream: Earth Beauty & You",
        "Oils: Clariss",
        "Oils: Earth Beauty & You",
        "Perfume 50ML: Assorted",
        "Perfume: 30ML",
        "Perfumes: Armaf",
        "Pomace Olive Oil: RS",
        "Shampoo: Clariss",
        "Shower Gel 380ML: Earth Beauty & You",
        "Sunflower Oil: RS",
        "Sunscreen: Earth Beauty & You",
      ];

      try {
        const categories = categoryArray.map((name) => ({ name }));
        await category.insertMany(categories);
        res
          .status(201)
          .json({ message: "Bulk categories inserted successfully" });
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // 🔹 Get all category names
    app.get("/categories", async (req, res) => {
      try {
        const categories = await category
          .find({}, { projection: { name: 1 } })
          .toArray();
        res.status(200).json(categories.map((c) => c.name));
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });
    // 🔹 Get all categories
    app.get("/all-category", async (req, res) => {
      try {
        const categories = await category.find({}).toArray();
        res.status(200).json(categories);
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // 🔹 Add a single category
    app.post("/categories", async (req, res) => {
      try {
        const { name } = req.body;
        if (!name)
          return res.status(400).json({ error: "Category name required" });

        await category.insertOne({ name });
        res.status(201).json({ message: "Category added" });
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // 🔹 Update a category by ID
    app.put("/categories/:id", async (req, res) => {
      try {
        const { id } = req.params;
        const { name } = req.body;

        if (!name)
          return res.status(400).json({ error: "New category name required" });

        const result = await category.updateOne(
          { _id: new ObjectId(id) },
          { $set: { name } },
        );

        if (result.modifiedCount === 0) {
          return res
            .status(404)
            .json({ error: "Category not found or unchanged" });
        }

        res.status(200).json({ message: "Category updated" });
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // Endpoint to update category in all products
    app.put("/update-products-category", async (req, res) => {
      try {
        const { oldCategory, newCategory } = req.body;

        if (!oldCategory || !newCategory) {
          return res
            .status(400)
            .json({ error: "Both old and new category names are required" });
        }

        const result = await products.updateMany(
          { category: oldCategory },
          { $set: { category: newCategory } },
        );

        res.status(200).json({
          message: `Updated ${result.modifiedCount} products`,
          modifiedCount: result.modifiedCount,
        });
      } catch (error) {
        console.error("Error updating products category:", error);
        res.status(500).json({ error: error.message });
      }
    });

    // Get all unique categories
    app.get("/product-categories", async (req, res) => {
      try {
        const categories = await products.distinct("category");
        res.status(200).json(categories);
      } catch (error) {
        console.error("Error fetching categories:", error);
        res.status(500).json({ message: "Error fetching categories", error });
      }
    });

    // Bulk update products by category
    app.put("/category-bulk-update", async (req, res) => {
      try {
        const { category, updateFields } = req.body;

        const result = await products.updateMany(
          { category },
          { $set: updateFields },
        );

        res.status(200).json({
          message: `Updated ${result.modifiedCount} products in category ${category}`,
          modifiedCount: result.modifiedCount,
        });
      } catch (error) {
        console.error("Error in bulk category update:", error);
        res
          .status(500)
          .json({ message: "Error updating category products", error });
      }
    });

    // 🔹 Sync brands from products to brand collection
    app.post("/sync-brands", async (req, res) => {
      try {
        // Get all unique brands from products
        const brandsFromProducts = await products.distinct("brand");
        const filteredBrands = brandsFromProducts.filter((b) => b); // Remove null/empty values

        if (filteredBrands.length === 0) {
          return res.status(400).json({ error: "No brands found in products" });
        }

        // Get existing brands to avoid duplicates
        const existingBrands = await brand.find({}).toArray();
        const existingBrandNames = existingBrands.map((b) => b.name);

        // Find brands that don't exist yet
        const newBrands = filteredBrands
          .filter((name) => !existingBrandNames.includes(name))
          .map((name) => ({ name }));

        if (newBrands.length === 0) {
          return res
            .status(200)
            .json({ message: "All brands already exist in collection" });
        }

        // Insert new brands
        const result = await brand.insertMany(newBrands);

        res.status(201).json({
          message: `Added ${result.insertedCount} new brands`,
          insertedCount: result.insertedCount,
          brands: newBrands.map((b) => b.name),
        });
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });
    // 🔹 Get all brand names
    app.get("/brands", async (req, res) => {
      try {
        const brands = await brand
          .find({}, { projection: { name: 1 } })
          .toArray();
        res.status(200).json(brands.map((b) => b.name));
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // 🔹 Get all brands with full details
    app.get("/all-brands", async (req, res) => {
      try {
        const brands = await brand.find({}).toArray();
        res.status(200).json(brands);
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // 🔹 Add a single brand
    app.post("/brands", async (req, res) => {
      try {
        const { name } = req.body;
        if (!name)
          return res.status(400).json({ error: "Brand name required" });

        await brand.insertOne({ name });
        res.status(201).json({ message: "Brand added" });
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // 🔹 Update a brand by ID
    app.put("/brands/:id", async (req, res) => {
      try {
        const { id } = req.params;
        const { name } = req.body;

        if (!name)
          return res.status(400).json({ error: "New brand name required" });

        const result = await brand.updateOne(
          { _id: new ObjectId(id) },
          { $set: { name } },
        );

        if (result.modifiedCount === 0) {
          return res
            .status(404)
            .json({ error: "Brand not found or unchanged" });
        }

        res.status(200).json({ message: "Brand updated" });
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // Endpoint to update brand in all products
    app.put("/update-products-brand", async (req, res) => {
      try {
        const { oldBrand, newBrand } = req.body;

        if (!oldBrand || !newBrand) {
          return res
            .status(400)
            .json({ error: "Both old and new brand names are required" });
        }

        const result = await products.updateMany(
          { brand: oldBrand },
          { $set: { brand: newBrand } },
        );

        res.status(200).json({
          message: `Updated ${result.modifiedCount} products`,
          modifiedCount: result.modifiedCount,
        });
      } catch (error) {
        console.error("Error updating products brand:", error);
        res.status(500).json({ error: error.message });
      }
    });

    // Get unique outlets from users collection
    app.get("/api/unique-outlets", async (req, res) => {
      try {
        const outlets = await users.distinct("outlet", { outlet: { $ne: "" } });
        res.json({ success: true, data: outlets.filter((outlet) => outlet) });
      } catch (error) {
        console.error("Error fetching outlets:", error);
        res
          .status(500)
          .json({ success: false, message: "Error fetching outlets" });
      }
    });

    // Get outlet stock by product barcode and outlet name
    app.get("/outlet-stock", async (req, res) => {
      try {
        const { barcode, outlet } = req.query;
        if (!barcode || !outlet) {
          return res
            .status(400)
            .json({ message: "barcode and outlet are required" });
        }

        // Decode the outlet name if it was URL encoded
        const decodedOutlet = decodeURIComponent(outlet);

        const stockDoc = await outletStockCollection.findOne({
          barcode: barcode,
        });

        if (
          stockDoc &&
          stockDoc.outlet_stocks &&
          stockDoc.outlet_stocks[decodedOutlet] !== undefined
        ) {
          res
            .status(200)
            .json({ stock: stockDoc.outlet_stocks[decodedOutlet] });
        } else {
          // Return default stock values if not found
          res.status(200).json({
            stock: {
              currentStock: 0,
              currentStockValueDP: 0,
              currentStockValueTP: 0,
            },
          });
        }
      } catch (error) {
        console.error("Error fetching outlet stock:", error);
        res.status(500).json({ message: "Error fetching outlet stock", error });
      }
    });
    // Get calculated opening stock for any date range
    app.get("/calculated-opening-stocks", async (req, res) => {
      try {
        const { outlet, endDate } = req.query;

        if (!outlet || !endDate) {
          return res.status(400).json({
            success: false,
            message: "Outlet and endDate are required",
          });
        }

        // 1. Get the fixed opening stock values
        const fixedOpeningStocks = await outletStockCollection
          .find({})
          .toArray();
        const fixedOpeningMap = fixedOpeningStocks.reduce((acc, stock) => {
          const outletStock = stock.outlet_stocks[outlet] || {};
          acc[stock.barcode] = {
            openingStock: outletStock.openingStock || 0,
            openingValueDP: outletStock.openingStockValueDP || 0,
            openingValueTP: outletStock.openingStockValueTP || 0,
          };
          return acc;
        }, {});

        // 2. Get all transactions before the endDate
        const transactions = await stock_transactions
          .find({
            outlet: outlet,
            date: { $lt: endDate }, // All transactions before the report start date
          })
          .toArray();

        // 3. Calculate net changes for each product
        const netChanges = {};
        transactions.forEach((transaction) => {
          if (!netChanges[transaction.barcode]) {
            netChanges[transaction.barcode] = {
              primary: 0,
              secondary: 0,
              marketReturn: 0,
              officeReturn: 0,
            };
          }

          switch (transaction.type.toLowerCase()) {
            case "primary":
              netChanges[transaction.barcode].primary += transaction.quantity;
              break;
            case "secondary":
              netChanges[transaction.barcode].secondary += transaction.quantity;
              break;
            case "market return":
              netChanges[transaction.barcode].marketReturn +=
                transaction.quantity;
              break;
            case "office return":
              netChanges[transaction.barcode].officeReturn +=
                transaction.quantity;
              break;
          }
        });

        // 4. Combine with fixed opening and calculate actual opening
        const result = Object.keys(fixedOpeningMap).map((barcode) => {
          const fixed = fixedOpeningMap[barcode];
          const changes = netChanges[barcode] || {
            primary: 0,
            secondary: 0,
            marketReturn: 0,
            officeReturn: 0,
          };

          const openingQty =
            fixed.openingStock +
            changes.primary +
            changes.marketReturn -
            changes.secondary -
            changes.officeReturn;

          return {
            barcode: barcode,
            openingStock: openingQty,
            openingStockValueDP:
              openingQty *
              (fixed.openingValueDP / Math.max(1, fixed.openingStock)),
            openingStockValueTP:
              openingQty *
              (fixed.openingValueTP / Math.max(1, fixed.openingStock)),
          };
        });

        res.status(200).json({
          success: true,
          outlet,
          asOfDate: endDate,
          data: result,
        });
      } catch (error) {
        console.error("Error calculating opening stocks:", error);
        res.status(500).json({
          success: false,
          message: "Error calculating opening stocks",
          error: error.message,
        });
      }
    });

    app.put("/update-outlet-barcode", async (req, res) => {
      const { oldBarcode, newBarcode } = req.body;

      try {
        const result = await outletStockCollection.updateOne(
          { barcode: oldBarcode },
          { $set: { barcode: newBarcode } },
        );

        res.json({ success: true, message: "Outlet barcodes updated", result });
      } catch (error) {
        console.error("Error updating outlet barcodes:", error);
        res.status(500).json({ success: false, message: "Update failed" });
      }
    });

    app.put("/update-outlet-stock", async (req, res) => {
      const {
        barcode,
        outlet,
        newStock,
        currentStockValueDP,
        currentStockValueTP,
        openingStockValueTP,
        openingStockValueDP,
      } = req.body;

      try {
        // Build the update fields dynamically
        const updateFields = {
          [`outlet_stocks.${outlet}.currentStock`]: newStock,
          // [`outlet_stocks.${outlet}.currentStockValueDP`]: currentStockValueDP,
          // [`outlet_stocks.${outlet}.currentStockValueTP`]: currentStockValueTP,
        };

        // Only add opening stock values if they are defined
        if (openingStockValueDP !== undefined) {
          // updateFields[`outlet_stocks.${outlet}.openingStockValueDP`] =
          //   openingStockValueDP;
          updateFields[`outlet_stocks.${outlet}.openingStock`] = newStock;
        }

        if (openingStockValueTP !== undefined) {
          // updateFields[`outlet_stocks.${outlet}.openingStockValueTP`] =
          //   openingStockValueTP;
        }

        const result = await outletStockCollection.updateOne(
          { barcode: barcode },
          { $set: updateFields },
        );

        if (result.modifiedCount === 0) {
          return res.status(404).json({ message: "Stock not updated" });
        }

        res.json({ message: "Stock updated successfully" });
      } catch (error) {
        console.error("Error updating stock:", error);
        res.status(500).json({ message: "Failed to update stock" });
      }
    });

    app.put("/update-due", async (req, res) => {
      const { outlet, currentDue, isOpeningVoucher, newOpeningDue } = req.body;

      try {
        // First get the current opening due to calculate the difference
        const outletData = await outlet_collection.findOne({
          outlet_name: outlet,
        });

        let updateData = {};

        if (isOpeningVoucher) {
          // For opening voucher, we need to adjust both openingDue and current_due
          updateData = {
            opening_due: (outletData?.opening_due || 0) + newOpeningDue,
            current_due: (outletData?.current_due || 0) + newOpeningDue,
          };
        } else {
          // For regular vouchers (primary, payment, etc)
          updateData = { current_due: currentDue };
        }

        const result = await outlet_collection.updateOne(
          { outlet_name: outlet },
          {
            $set: updateData,
            // $setOnInsert: {
            //   opening_due: isOpeningVoucher ? newOpeningDue : 0,
            //   current_due: isOpeningVoucher ? newOpeningDue : currentDue,
            // },
          },
          { upsert: true },
        );

        res.json({
          success: true,
          message: "Due updated successfully",
        });
      } catch (error) {
        console.error("Error updating due:", error);
        res.status(500).json({
          success: false,
          message: "Failed to update due",
        });
      }
    });
    // Update outlet stock after a sale
    app.post("/add-sale-report", async (req, res) => {
      try {
        const {
          user,
          so,
          asm,
          rsm,
          som,
          zone,
          outlet,
          sale_date,
          total_tp,
          total_mrp,
          total_dp,
          products,
          customer,
          route,
          memo,
        } = req.body;

        // if (
        //   !user ||
        //   !outlet ||
        //   !sale_date ||
        //   !products ||
        //   products.length === 0
        // ) {
        //   return res.status(400).json({ message: "Invalid sale data" });
        // }

        // // Insert a single sale record containing all products
        // const saleEntry = {
        //   user,
        //   so,
        //   asm,
        //   rsm,
        //   som,
        //   zone,
        //   outlet,
        //   route,
        //   memo,
        //   sale_date,
        //   total_tp,
        //   total_mrp,
        //   total_dp,
        //   products,
        //   customer,
        // };
        // await salesCollection.insertOne(saleEntry);

        // Update outlet stock for each product
        for (const product of products) {
          const { barcode, quantity } = product;
          await outletStockCollection.updateOne(
            { barcode },
            {
              $inc: {
                [`outlet_stocks.${outlet}.currentStock`]: -quantity,
                [`outlet_stocks.${outlet}.currentStockValueDP`]: -product.dp,
                [`outlet_stocks.${outlet}.currentStockValueTP`]: -product.tp,
              },
            },
          );
        }

        res.status(200).json({
          message: "Outlet stock and sales report updated successfully",
        });
      } catch (error) {
        console.error("Error updating outlet stock and sales report:", error);
        res.status(500).json({
          message: "Error updating outlet stock and sales report",
          error,
        });
      }
    });

    app.post("/login", async (req, res) => {
      const { number, password } = req.body;

      try {
        // Fetch user from the database
        const user = await users.findOne({ number });

        if (!user) {
          return res.status(404).json({ message: "User not found" });
        }

        // Verify password (use bcrypt for hashing comparison in production)
        const isPasswordCorrect = password == user.password; // In production, replace with bcrypt comparison

        if (!isPasswordCorrect) {
          return res.status(401).json({ message: "Invalid email or password" });
        }

        // Return user details including the checked-in status
        res.status(200).json({
          message: "Login successful",
          user: user,
        });
      } catch (error) {
        console.error("Error during login:", error);
        res.status(500).json({ message: "Internal server error" });
      }
    });

    // Add this to your backend API
    app.get("/get-user-field-values", async (req, res) => {
      try {
        const { field } = req.query;

        if (!field) {
          return res
            .status(400)
            .send({ message: "Field parameter is required" });
        }

        const values = await users.distinct(field);
        res.status(200).send(values.filter((v) => v)); // Filter out null/undefined
      } catch (error) {
        console.error(`Error fetching ${field} values:`, error);
        res.status(500).send({ message: "Internal Server Error" });
      }
    });

    app.get("/getAllUser", async (req, res) => {
      try {
        const { role, group, zone } = req.query; // Get filters from query parameters
        let query = {};

        // Add filters using case-insensitive partial matching ($regex with "i" flag)
        if (role) {
          query.role = { $regex: role, $options: "i" };
        }
        if (group) {
          query.group = { $regex: group, $options: "i" };
        }
        if (zone) {
          query.zone = { $regex: zone, $options: "i" };
        }

        const user = await users.find(query).toArray();

        if (user.length > 0) {
          res.status(200).send(user);
        } else {
          res
            .status(404)
            .send({ message: "No users found with the given filters" });
        }
      } catch (error) {
        console.error("Error fetching users:", error);
        res.status(500).send({ message: "Internal Server Error" });
      }
    });

    app.post("/api/users", async (req, res) => {
      const newUser = req.body;
      const result = await users.insertOne(newUser);
      res.send(result);
    });

    app.delete("/api/users/:id", async (req, res) => {
      try {
        const { id } = req.params;
        const result = await users.deleteOne({ _id: new ObjectId(id) });
        if (result.deletedCount === 1) {
          res.status(200).send({ message: "User deleted successfully" });
        } else {
          res.status(404).send({ message: "User not found" });
        }
      } catch (error) {
        console.error("Error deleting user:", error);
        res.status(500).send({ message: "Failed to delete user" });
      }
    });

    app.get("/getUser/:userId", async (req, res) => {
      const userId = req.params.userId;

      try {
        const user = await users.findOne({ _id: new ObjectId(userId) });

        if (user) {
          res.status(200).send(user);
        } else {
          res.status(404).send({ message: "User not found" });
        }
      } catch (error) {
        console.error("Error fetching user:", error);
        res.status(500).send({ message: "Internal Server Error" });
      }
    });

    app.put("/updateUser/:userId", async (req, res) => {
      const userId = req.params.userId.trim(); // Extract and trim userId
      let updatedData = req.body;

      try {
        // Remove the _id field from updatedData if it exists
        delete updatedData._id;

        // Create possible match conditions
        const matchQuery = [{ _id: userId }]; // String match

        // Add ObjectId match if valid
        if (ObjectId.isValid(userId)) {
          matchQuery.push({ _id: new ObjectId(userId) });
        }

        // Update the user in the database
        const result = await users.updateOne(
          { $or: matchQuery },
          { $set: updatedData },
        );

        if (result.modifiedCount > 0) {
          res.status(200).send({ message: "User updated successfully" });
        } else if (result.matchedCount > 0) {
          res.status(200).send({ message: "No changes made to the user" });
        } else {
          res.status(404).send({ message: "User not found" });
        }
      } catch (error) {
        console.error("Error updating user:", error);
        res.status(500).send({ message: "Internal Server Error" });
      }
    });
    // Updated GET /targets endpoint
    app.get("/targets", async (req, res) => {
      const { year, month } = req.query;
      try {
        const targets = await targetsCollection
          .find({
            "targets.year": parseInt(year),
            "targets.month": parseInt(month),
          })
          .toArray();

        res.json(targets);
      } catch (error) {
        console.error("Error fetching targets:", error);
        res.status(500).json({ message: "Server Error", error: error.message });
      }
    });

    // Updated POST /targets endpoint
    app.post("/targets", async (req, res) => {
      const { year, month, targets } = req.body;

      try {
        // Validate input
        if (!year || !month || !targets || !Array.isArray(targets)) {
          return res.status(400).json({ message: "Invalid request body" });
        }

        const bulkOps = targets.map((targetEntry) => {
          const { userID, userName, userNumber, userZone, tp, dp } =
            targetEntry;

          if (tp === undefined || dp === undefined) {
            throw new Error(`Missing target values for user ${userID}`);
          }

          return {
            updateOne: {
              filter: { userID: userID },
              update: {
                $set: {
                  userName: userName,
                  userNumber: userNumber,
                  userZone: userZone,
                },
                $push: {
                  targets: {
                    year: parseInt(year),
                    month: parseInt(month),
                    tp: parseFloat(tp),
                    dp: parseFloat(dp),
                  },
                },
              },
              upsert: true,
            },
          };
        });

        const result = await targetsCollection.bulkWrite(bulkOps);

        res.json({
          message: "Targets created successfully",
          insertedCount: result.upsertedCount,
          modifiedCount: result.modifiedCount,
        });
      } catch (error) {
        console.error("Error creating targets:", error);
        res.status(500).json({
          message: error.message || "Error creating targets",
          error: error.message,
        });
      }
    });

    // Updated PUT /targets endpoint
    app.put("/targets", async (req, res) => {
      const { userID, userName, userNumber, userZone, year, month, tp, dp } =
        req.body;

      if (!userID || !year || !month || tp === undefined || dp === undefined) {
        return res.status(400).json({ message: "Missing required fields" });
      }

      try {
        // Update both the target and user details
        const result = await targetsCollection.updateOne(
          {
            userID: userID,
            "targets.year": parseInt(year),
            "targets.month": parseInt(month),
          },
          {
            $set: {
              userName: userName,
              userNumber: userNumber,
              userZone: userZone,
              "targets.$[elem].tp": tp,
              "targets.$[elem].dp": dp,
            },
          },
          {
            arrayFilters: [
              {
                "elem.year": parseInt(year),
                "elem.month": parseInt(month),
              },
            ],
          },
        );

        if (result.matchedCount === 0) {
          return res.status(404).json({
            message: "Target not found for this user, year, and month",
          });
        }

        res.json({
          message: "Target updated successfully",
          modifiedCount: result.modifiedCount,
        });
      } catch (error) {
        console.error("Error updating target:", error);
        res.status(500).json({
          message: "Error updating target",
          error: error.message,
        });
      }
    });
    // Bulk update targets with user details
    app.put("/targets/bulk-update", async (req, res) => {
      try {
        // Get all targets
        const allTargets = await targetsCollection.find({}).toArray();

        // Process each target
        const bulkOps = [];

        for (const target of allTargets) {
          // Get user details
          const user = await users.findOne({
            _id: target.userID,
          });

          if (user) {
            bulkOps.push({
              updateOne: {
                filter: { _id: target._id },
                update: {
                  $set: {
                    userName: user.name,
                    userNumber: user.number,
                    userZone: user.zone,
                  },
                },
              },
            });
          }
        }

        // Execute bulk operation
        if (bulkOps.length > 0) {
          const result = await targetsCollection.bulkWrite(bulkOps);
          res.json({
            message: "Bulk update successful",
            modifiedCount: result.modifiedCount,
          });
        } else {
          res.json({ message: "No targets to update" });
        }
      } catch (error) {
        console.error("Error in bulk update:", error);
        res
          .status(500)
          .json({ message: "Error in bulk update", error: error.message });
      }
    });

    app.get("/categoryTargets", async (req, res) => {
      try {
        const { year, month, userID } = req.query;
        const query = {};

        // Handle userID - support both single ID and comma-separated IDs
        if (userID) {
          query.userID = {
            $in: userID.includes(",") ? userID.split(",") : [userID],
          };
        }

        // console.log("Query parameters:", { year, month, userID });

        const targets = await categoryTargetsCollection.find(query).toArray();

        // Debug log to see raw data before filtering
        // console.log("Raw targets data:", JSON.stringify(targets, null, 2));

        // Filter results if year/month provided
        const filtered = targets
          .map((doc) => {
            const filteredTargets = doc.targets.filter((target) => {
              let match = true;
              if (year) match = match && target.year === parseInt(year);
              if (month) match = match && target.month === parseInt(month);
              return match;
            });
            return { ...doc, targets: filteredTargets };
          })
          .filter((doc) => doc.targets.length > 0);

        // console.log("Filtered results:", JSON.stringify(filtered, null, 2));

        res.status(200).json(filtered);
      } catch (err) {
        console.error("Error in /categoryTargets:", err);
        res.status(500).json({ error: err.message });
      }
    });

    // Create or update category targets
    app.post("/categoryTargets", async (req, res) => {
      try {
        const { userID, year, month, targets } = req.body;

        // Validate input
        if (!userID || !year || !month || !targets || !Array.isArray(targets)) {
          return res.status(400).json({ error: "Invalid input data" });
        }

        // Check if target exists for this user/month/year
        const existingTarget = await categoryTargetsCollection.findOne({
          userID,
          "targets.year": parseInt(year),
          "targets.month": parseInt(month),
        });

        if (existingTarget) {
          // Update existing
          const result = await categoryTargetsCollection.updateOne(
            {
              userID,
              "targets.year": parseInt(year),
              "targets.month": parseInt(month),
            },
            {
              $set: {
                "targets.$.targets": targets,
                "targets.$.updatedAt": new Date(),
              },
            },
          );
          res.status(200).json({
            message: "Targets updated",
            modifiedCount: result.modifiedCount,
          });
        } else {
          // Create new
          const result = await categoryTargetsCollection.updateOne(
            { userID },
            {
              $push: {
                targets: {
                  year: parseInt(year),
                  month: parseInt(month),
                  targets,
                  createdAt: new Date(),
                  updatedAt: new Date(),
                },
              },
            },
            { upsert: true },
          );
          res.status(201).json({
            message: "Targets created",
            upsertedId: result.upsertedId,
          });
        }
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // Delete category targets for specific period
    app.delete("/categoryTargets", async (req, res) => {
      try {
        const { userID, year, month } = req.query;

        if (!userID || !year || !month) {
          return res.status(400).json({ error: "Missing required parameters" });
        }

        const result = await categoryTargetsCollection.updateOne(
          { userID },
          {
            $pull: {
              targets: {
                year: parseInt(year),
                month: parseInt(month),
              },
            },
          },
        );

        res.status(200).json({
          message: "Targets deleted",
          modifiedCount: result.modifiedCount,
        });
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    app.get("/sales-reports/:userId", async (req, res) => {
      try {
        const { userId } = req.params;
        const { month, startDate, endDate } = req.query;

        let filter = { user: userId };

        if (startDate && endDate) {
          // Format custom date range using strings
          const formattedStartDate = dayjs(startDate, "YYYY-MM-DD").format(
            "YYYY-MM-DD HH:mm:ss",
          );
          const formattedEndDate = dayjs(endDate, "YYYY-MM-DD")
            .endOf("day")
            .format("YYYY-MM-DD HH:mm:ss");

          filter.sale_date = {
            $gte: formattedStartDate,
            $lte: formattedEndDate,
          };
        } else if (month) {
          // Default: Month-wise filter (same format as the working API)
          const startOfMonth = dayjs(month, "YYYY-MM")
            .startOf("month")
            .format("YYYY-MM-DD HH:mm:ss");
          const endOfMonth = dayjs(month, "YYYY-MM")
            .endOf("month")
            .format("YYYY-MM-DD HH:mm:ss");

          filter.sale_date = { $gte: startOfMonth, $lte: endOfMonth };
        } else {
          return res
            .status(400)
            .json({ message: "Provide month or custom date range" });
        }

        // Fetch filtered reports
        const reports = await salesCollection.find(filter).toArray();
        res.status(200).json(reports);
      } catch (error) {
        console.error("Error fetching sales reports:", error);
        res
          .status(500)
          .json({ message: "Error fetching sales reports", error });
      }
    });

    // New endpoint to get all sales reports with user info
    app.get("/sales-reports", async (req, res) => {
      try {
        const { month, startDate, endDate } = req.query;

        let filter = {};

        if (startDate && endDate) {
          const formattedStartDate = dayjs(startDate, "YYYY-MM-DD").format(
            "YYYY-MM-DD HH:mm:ss",
          );
          const formattedEndDate = dayjs(endDate, "YYYY-MM-DD")
            .endOf("day")
            .format("YYYY-MM-DD HH:mm:ss");

          filter.sale_date = {
            $gte: formattedStartDate,
            $lte: formattedEndDate,
          };
        } else if (month) {
          const startOfMonth = dayjs(month, "YYYY-MM")
            .startOf("month")
            .format("YYYY-MM-DD HH:mm:ss");
          const endOfMonth = dayjs(month, "YYYY-MM")
            .endOf("month")
            .format("YYYY-MM-DD HH:mm:ss");

          filter.sale_date = { $gte: startOfMonth, $lte: endOfMonth };
        } else {
          return res
            .status(400)
            .json({ message: "Provide month or custom date range" });
        }

        const reports = await salesCollection.find(filter).toArray();

        res.status(200).json(reports);
      } catch (error) {
        console.error("Error fetching sales reports:", error);
        res
          .status(500)
          .json({ message: "Error fetching sales reports", error });
      }
    });

    app.put("/update-sales-report/:reportId", async (req, res) => {
      try {
        const { reportId } = req.params;
        const updatedData = req.body;

        // 1. Get original report
        const originalReport = await salesCollection.findOne({
          _id: new ObjectId(reportId),
        });

        if (!originalReport) {
          return res.status(404).json({ error: "Report not found" });
        }

        // 2. Check if report is from today
        const reportDate = dayjs(originalReport.sale_date);
        const today = dayjs().startOf("day");

        if (reportDate.isBefore(today, "day")) {
          return res
            .status(400)
            .json({ error: "Cannot edit reports older than today" });
        }
        // 3. Map original quantities
        const originalQuantities = {};
        originalReport.products.forEach((product) => {
          originalQuantities[product.barcode] = product.quantity;
        });

        // 4. Process each product update
        for (const updatedProduct of updatedData.products) {
          const originalQty = originalQuantities[updatedProduct.barcode] || 0;
          const quantityDiff = updatedProduct.quantity - originalQty;

          if (quantityDiff !== 0) {
            // Update outlet stock
            await outletStockCollection.updateOne(
              {
                name: originalReport.outlet,
                "products.barcode": updatedProduct.barcode,
              },
              {
                $inc: {
                  "products.$.stock": -quantityDiff,
                },
              },
            );

            // Update stock transaction
            await stock_transactions.updateOne(
              {
                barcode: updatedProduct.barcode,
                outlet: originalReport.outlet,
                date: originalReport.sale_date,
                type: "secondary",
              },
              {
                $set: {
                  quantity: updatedProduct.quantity,
                  dp: updatedProduct.dp / updatedProduct.quantity,
                  tp: updatedProduct.tp / updatedProduct.quantity,
                  updatedAt: new Date(),
                },
              },
              { upsert: true },
            );
          }
        }

        delete updatedData._id;

        // 5. Update the sales report
        await salesCollection.updateOne(
          { _id: new ObjectId(reportId) },
          { $set: updatedData },
        );

        res.status(200).json({ message: "Report updated successfully" });
      } catch (error) {
        console.error("Error updating sales report:", error);
        res.status(500).json({
          message: "Error updating sales report",
          error: error.message,
        });
      }
    });

    app.delete("/delete-sales-report/:reportId", async (req, res) => {
      const { reportId } = req.params;

      try {
        // 1. Get the report to delete
        const report = await db
          .collection("sales")
          .findOne({ _id: new ObjectId(reportId) });

        if (!report) {
          return res.status(404).json({ error: "Report not found" });
        }

        // 2. Check if report is from today
        const reportDate = dayjs(report.sale_date);
        const today = dayjs().startOf("day");

        if (reportDate.isBefore(today, "day")) {
          return res
            .status(400)
            .json({ error: "Cannot delete reports older than today" });
        }

        // 3. Revert stock changes
        for (const product of report.products) {
          // Update outlet stock
          await db.collection("outlets").updateOne(
            {
              name: report.outlet,
              "products.barcode": product.barcode,
            },
            {
              $inc: {
                "products.$.stock": product.quantity,
              },
            },
          );

          // Delete stock transaction
          await db.collection("stockTransactions").deleteOne({
            barcode: product.barcode,
            outlet: report.outlet,
            date: report.sale_date,
            type: "secondary",
          });
        }

        // 4. Delete the sales report
        await db.collection("sales").deleteOne({ _id: new ObjectId(reportId) });

        res.status(200).json({ message: "Report deleted successfully" });
      } catch (error) {
        console.error("Delete Sales Report Error:", error);
        res
          .status(500)
          .json({ error: "An error occurred while deleting the report" });
      }
    });

    // Get brand targets for a specific period
    app.get("/brandTargets", async (req, res) => {
      try {
        const { year, month, userID } = req.query;
        const query = {};

        // Handle userID - support both single ID and comma-separated IDs
        if (userID) {
          query.userID = {
            $in: userID.includes(",") ? userID.split(",") : [userID],
          };
        }

        const targets = await brandTargets.find(query).toArray();

        // Filter results if year/month provided
        const filtered = targets
          .map((doc) => {
            const filteredTargets = doc.targets.filter((target) => {
              let match = true;
              if (year) match = match && target.year === parseInt(year);
              if (month) match = match && target.month === parseInt(month);
              return match;
            });
            return { ...doc, targets: filteredTargets };
          })
          .filter((doc) => doc.targets.length > 0);

        res.status(200).json(filtered);
      } catch (err) {
        console.error("Error in /brandTargets:", err);
        res.status(500).json({ error: err.message });
      }
    });
    // Create or update brand targets
    app.post("/brandTargets", async (req, res) => {
      try {
        const { userID, year, month, targets } = req.body;

        // Validate input
        if (!userID || !year || !month || !targets || !Array.isArray(targets)) {
          return res.status(400).json({ error: "Invalid input data" });
        }

        // Check if target exists for this user/month/year
        const existingTarget = await brandTargets.findOne({
          userID,
          "targets.year": parseInt(year),
          "targets.month": parseInt(month),
        });

        if (existingTarget) {
          // Update existing
          const result = await brandTargets.updateOne(
            {
              userID,
              "targets.year": parseInt(year),
              "targets.month": parseInt(month),
            },
            {
              $set: {
                "targets.$.targets": targets,
                "targets.$.updatedAt": new Date(),
              },
            },
          );
          res.status(200).json({
            message: "Targets updated",
            modifiedCount: result.modifiedCount,
          });
        } else {
          // Create new
          const result = await brandTargets.updateOne(
            { userID },
            {
              $push: {
                targets: {
                  year: parseInt(year),
                  month: parseInt(month),
                  targets,
                  createdAt: new Date(),
                  updatedAt: new Date(),
                },
              },
            },
            { upsert: true },
          );
          res.status(201).json({
            message: "Targets created",
            upsertedId: result.upsertedId,
          });
        }
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    // Delete brand targets for specific period
    app.delete("/brandTargets", async (req, res) => {
      try {
        const { userID, year, month } = req.query;

        if (!userID || !year || !month) {
          return res.status(400).json({ error: "Missing required parameters" });
        }

        const result = await brandTargets.updateOne(
          { userID },
          {
            $pull: {
              targets: {
                year: parseInt(year),
                month: parseInt(month),
              },
            },
          },
        );

        res.status(200).json({
          message: "Targets deleted",
          modifiedCount: result.modifiedCount,
        });
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    app.get("/daily-sales", async (req, res) => {
      const { month } = req.query;
      const targetMonth = month || dayjs().format("YYYY-MM");

      const startDate = dayjs(targetMonth + "-01 00:00:00");
      const endDate = startDate.endOf("month");

      try {
        const pipeline = [
          {
            $match: {
              sale_date: {
                $gte: startDate.format("YYYY-MM-DD HH:mm:ss"),
                $lte: endDate.format("YYYY-MM-DD HH:mm:ss"),
              },
            },
          },
          {
            $addFields: {
              day: {
                $dateToString: {
                  format: "%Y-%m-%d",
                  date: {
                    $dateFromString: {
                      dateString: "$sale_date",
                      format: "%Y-%m-%d %H:%M:%S",
                    },
                  },
                },
              },
            },
          },
          {
            $group: {
              _id: "$day",
              total_tp: { $sum: "$total_tp" },
              total_mrp: { $sum: "$total_mrp" },
            },
          },
          {
            $sort: { _id: 1 },
          },
        ];

        const result = await salesCollection.aggregate(pipeline).toArray();

        res.json({
          month: targetMonth,
          sales: result.map((item) => ({
            date: item._id,
            total_tp: item.total_tp,
            total_mrp: item.total_mrp,
          })),
        });
      } catch (err) {
        console.error("Error fetching daily sales:", err);
        res.status(500).json({ message: "Internal server error" });
      }
    });

    // Add this to your backend routes
    app.get("/sales/brand-wise", async (req, res) => {
      try {
        const { month, startDate, endDate } = req.query;
        let filter = {};

        if (startDate && endDate) {
          filter.sale_date = {
            $gte: dayjs(startDate).startOf("day").format("YYYY-MM-DD HH:mm:ss"),
            $lte: dayjs(endDate).endOf("day").format("YYYY-MM-DD HH:mm:ss"),
          };
        } else if (month) {
          filter.sale_date = {
            $gte: dayjs(month).startOf("month").format("YYYY-MM-DD HH:mm:ss"),
            $lte: dayjs(month).endOf("month").format("YYYY-MM-DD HH:mm:ss"),
          };
        }

        const reports = await salesCollection
          .aggregate([
            { $match: filter },
            { $unwind: "$products" },
            {
              $group: {
                _id: "$products.brand", // Group by brand instead of category
                total_quantity: { $sum: "$products.quantity" },
                total_mrp: { $sum: "$products.mrp" },
                total_tp: { $sum: "$products.tp" },
              },
            },
            { $sort: { _id: 1 } },
          ])
          .toArray();

        res.status(200).json(reports);
      } catch (error) {
        console.error("Error fetching brand-wise sales data:", error);
        res
          .status(500)
          .json({ message: "Error fetching brand-wise sales data", error });
      }
    });
    app.get("/sales/zone-wise", async (req, res) => {
      try {
        const { month, startDate, endDate } = req.query;
        let filter = {};

        if (startDate && endDate) {
          filter.sale_date = {
            $gte: dayjs(startDate).startOf("day").format("YYYY-MM-DD HH:mm:ss"),
            $lte: dayjs(endDate).endOf("day").format("YYYY-MM-DD HH:mm:ss"),
          };
        } else if (month) {
          filter.sale_date = {
            $gte: dayjs(month).startOf("month").format("YYYY-MM-DD HH:mm:ss"),
            $lte: dayjs(month).endOf("month").format("YYYY-MM-DD HH:mm:ss"),
          };
        }

        const reports = await salesCollection
          .aggregate([
            { $match: filter },
            { $unwind: "$products" },
            {
              $group: {
                _id: "$zone",
                total_quantity: { $sum: "$products.quantity" },
                total_mrp: { $sum: "$products.mrp" },
                total_tp: { $sum: "$products.tp" },
              },
            },
            { $sort: { _id: 1 } },
          ])
          .toArray();

        res.status(200).json(reports || []);
      } catch (error) {
        console.error("Error fetching sales data:", error);
        res.status(200).json([]);
      }
    });

    app.get("/targets/zone-wise", async (req, res) => {
      const { year, month } = req.query;

      try {
        const allTargets = await targetsCollection
          .find({
            "targets.year": parseInt(year),
            "targets.month": parseInt(month),
          })
          .toArray();

        const zoneTotals = {};

        allTargets.forEach((target) => {
          // Use the zone directly from the target document
          const zone = target.userZone;
          if (!zone) return;

          const monthlyTarget = target.targets.find(
            (t) => t.year === parseInt(year) && t.month === parseInt(month),
          );

          if (monthlyTarget) {
            if (!zoneTotals[zone]) {
              zoneTotals[zone] = {
                total_tp_target: 0,
                total_dp_target: 0,
              };
            }

            zoneTotals[zone].total_tp_target +=
              parseFloat(monthlyTarget.tp) || 0;
            zoneTotals[zone].total_dp_target +=
              parseFloat(monthlyTarget.dp) || 0;
          }
        });

        const result = Object.keys(zoneTotals).map((zone) => ({
          _id: zone,
          ...zoneTotals[zone],
        }));

        res.status(200).json(result || []);
      } catch (error) {
        console.error("Error fetching targets:", error);
        res.status(200).json([]);
      }
    });

    app.get("/sales/brand-wise/outlet-details", async (req, res) => {
      try {
        const { brand, month, startDate, endDate } = req.query;
        let filter = {};

        if (startDate && endDate) {
          filter.sale_date = {
            $gte: dayjs(startDate).startOf("day").format("YYYY-MM-DD HH:mm:ss"),
            $lte: dayjs(endDate).endOf("day").format("YYYY-MM-DD HH:mm:ss"),
          };
        } else if (month) {
          filter.sale_date = {
            $gte: dayjs(month).startOf("month").format("YYYY-MM-DD HH:mm:ss"),
            $lte: dayjs(month).endOf("month").format("YYYY-MM-DD HH:mm:ss"),
          };
        }

        // Add brand filter
        filter["products.brand"] = brand;

        const reports = await salesCollection
          .aggregate([
            { $match: filter },
            { $unwind: "$products" },
            { $match: { "products.brand": brand } },
            {
              $group: {
                _id: {
                  so: "$so",
                  userID: "$user",
                  outlet: "$outlet",
                },
                total_quantity: { $sum: "$products.quantity" },
                total_mrp: { $sum: "$products.mrp" },
                total_tp: { $sum: "$products.tp" },
              },
            },
            { $sort: { _id: 1 } },
          ])
          .toArray();

        res.status(200).json(reports);
      } catch (error) {
        console.error("Error fetching outlet details by brand:", error);
        res
          .status(500)
          .json({ message: "Error fetching outlet details by brand", error });
      }
    });

    app.get("/sales/brand-wise", async (req, res) => {
      try {
        const { month, startDate, endDate } = req.query;

        // Initialize filter object
        let filter = {};

        if (startDate && endDate) {
          // Format the custom date range to ensure proper filtering
          const formattedStartDate = dayjs(startDate, "YYYY-MM-DD")
            .startOf("day")
            .format("YYYY-MM-DD HH:mm:ss");
          const formattedEndDate = dayjs(endDate, "YYYY-MM-DD")
            .endOf("day")
            .format("YYYY-MM-DD HH:mm:ss");

          filter.sale_date = {
            $gte: formattedStartDate,
            $lte: formattedEndDate,
          };
        } else if (month) {
          // Handle the month filter
          const startOfMonth = dayjs(month, "YYYY-MM")
            .startOf("month")
            .format("YYYY-MM-DD HH:mm:ss");
          const endOfMonth = dayjs(month, "YYYY-MM")
            .endOf("month")
            .format("YYYY-MM-DD HH:mm:ss");

          filter.sale_date = { $gte: startOfMonth, $lte: endOfMonth };
        } else {
          // If neither month nor date range is provided, return an error
          return res
            .status(400)
            .json({ message: "Please provide a valid month or date range" });
        }

        // Fetch the sales data based on the filter
        const reports = await salesCollection
          .aggregate([
            { $match: filter },
            { $unwind: "$products" }, // Unwind the products array to group by category
            {
              $group: {
                _id: "$products.brand", // Group by category
                total_quantity: { $sum: "$products.quantity" }, // Sum of quantities sold
                total_mrp: { $sum: "$products.mrp" }, // Total MRP for the category
                total_tp: { $sum: "$products.tp" }, // Total TP for the category
              },
            },
            { $sort: { _id: 1 } }, // Sort by category (optional)
          ])
          .toArray();

        res.status(200).json(reports);
      } catch (error) {
        console.error("Error fetching category-wise sales data:", error);
        res
          .status(500)
          .json({ message: "Error fetching category-wise sales data", error });
      }
    });
    app.get("/sales/category-wise", async (req, res) => {
      try {
        const { month, startDate, endDate } = req.query;

        // Initialize filter object
        let filter = {};

        if (startDate && endDate) {
          // Format the custom date range to ensure proper filtering
          const formattedStartDate = dayjs(startDate, "YYYY-MM-DD")
            .startOf("day")
            .format("YYYY-MM-DD HH:mm:ss");
          const formattedEndDate = dayjs(endDate, "YYYY-MM-DD")
            .endOf("day")
            .format("YYYY-MM-DD HH:mm:ss");

          filter.sale_date = {
            $gte: formattedStartDate,
            $lte: formattedEndDate,
          };
        } else if (month) {
          // Handle the month filter
          const startOfMonth = dayjs(month, "YYYY-MM")
            .startOf("month")
            .format("YYYY-MM-DD HH:mm:ss");
          const endOfMonth = dayjs(month, "YYYY-MM")
            .endOf("month")
            .format("YYYY-MM-DD HH:mm:ss");

          filter.sale_date = { $gte: startOfMonth, $lte: endOfMonth };
        } else {
          // If neither month nor date range is provided, return an error
          return res
            .status(400)
            .json({ message: "Please provide a valid month or date range" });
        }

        // Fetch the sales data based on the filter
        const reports = await salesCollection
          .aggregate([
            { $match: filter },
            { $unwind: "$products" }, // Unwind the products array to group by category
            {
              $group: {
                _id: "$products.category", // Group by category
                total_quantity: { $sum: "$products.quantity" }, // Sum of quantities sold
                total_mrp: { $sum: "$products.mrp" }, // Total MRP for the category
                total_tp: { $sum: "$products.tp" }, // Total TP for the category
              },
            },
            { $sort: { _id: 1 } }, // Sort by category (optional)
          ])
          .toArray();

        res.status(200).json(reports);
      } catch (error) {
        console.error("Error fetching category-wise sales data:", error);
        res
          .status(500)
          .json({ message: "Error fetching category-wise sales data", error });
      }
    });
    app.get("/sales/category-wise/outlet-details", async (req, res) => {
      try {
        const { category, month, startDate, endDate } = req.query;

        let filter = {};

        if (startDate && endDate) {
          filter.sale_date = {
            $gte: dayjs(startDate).startOf("day").format("YYYY-MM-DD HH:mm:ss"),
            $lte: dayjs(endDate).endOf("day").format("YYYY-MM-DD HH:mm:ss"),
          };
        } else if (month) {
          filter.sale_date = {
            $gte: dayjs(month).startOf("month").format("YYYY-MM-DD HH:mm:ss"),
            $lte: dayjs(month).endOf("month").format("YYYY-MM-DD HH:mm:ss"),
          };
        }

        // Add category filter
        filter["products.category"] = category;

        const reports = await salesCollection
          .aggregate([
            { $match: filter },
            { $unwind: "$products" },
            { $match: { "products.category": category } },
            {
              $group: {
                _id: {
                  so: "$so",
                  userID: "$user",
                  outlet: "$outlet",
                },
                total_quantity: { $sum: "$products.quantity" },
                total_mrp: { $sum: "$products.mrp" },
                total_tp: { $sum: "$products.tp" },
              },
            },
            { $sort: { _id: 1 } },
          ])
          .toArray();

        res.status(200).json(reports);
      } catch (error) {
        console.error("Error fetching outlet details:", error);
        res
          .status(500)
          .json({ message: "Error fetching outlet details", error });
      }
    });

    app.get("/api/sales/category/:category", async (req, res) => {
      try {
        const { category } = req.params;
        const products = await salesCollection
          .aggregate([
            { $unwind: "$products" },
            { $match: { "products.category": category } },
            {
              $group: {
                _id: "$products.product_name",
                barcode: { $first: "$products.barcode" },
                total_quantity: { $sum: "$products.quantity" },
                total_tp: { $sum: "$products.tp" },
                total_mrp: { $sum: "$products.mrp" },
              },
            },
            { $sort: { total_quantity: -1 } },
          ])
          .toArray();

        res.json(products);
      } catch (error) {
        res
          .status(500)
          .json({ message: "Error fetching products", error: error.message });
      }
    });

    app.get("/api/sales/product-wise", async (req, res) => {
      try {
        const { month, startDate, endDate } = req.query;
        let filter = {};

        if (startDate && endDate) {
          const formattedStartDate = dayjs(startDate, "YYYY-MM-DD")
            .startOf("day")
            .format("YYYY-MM-DD HH:mm:ss");
          const formattedEndDate = dayjs(endDate, "YYYY-MM-DD")
            .endOf("day")
            .format("YYYY-MM-DD HH:mm:ss");

          filter.sale_date = {
            $gte: formattedStartDate,
            $lte: formattedEndDate,
          };
        } else if (month) {
          const startOfMonth = dayjs(month, "YYYY-MM")
            .startOf("month")
            .format("YYYY-MM-DD HH:mm:ss");
          const endOfMonth = dayjs(month, "YYYY-MM")
            .endOf("month")
            .format("YYYY-MM-DD HH:mm:ss");

          filter.sale_date = { $gte: startOfMonth, $lte: endOfMonth };
        } else {
          return res
            .status(400)
            .json({ message: "Please provide a valid month or date range" });
        }

        const productWiseSales = await salesCollection
          .aggregate([
            { $match: filter },
            { $unwind: "$products" },
            {
              $group: {
                _id: "$products.product_name",
                barcode: { $first: "$products.barcode" },
                total_quantity: { $sum: "$products.quantity" },
                total_tp: { $sum: "$products.tp" },
                total_mrp: { $sum: "$products.mrp" },
                total_dp: { $sum: "$products.dp" },
              },
            },
            { $sort: { total_tp: -1 } },
          ])
          .toArray();

        res.status(200).json(productWiseSales);
      } catch (error) {
        console.error("Error fetching product-wise sales:", error);
        res
          .status(500)
          .json({ message: "Error fetching sales data", error: error.message });
      }
    });

    // Add this to your backend routes
    app.get("/api/sales/product-wise/outlet-details", async (req, res) => {
      try {
        const { productName, month, startDate, endDate } = req.query;
        let filter = {};

        if (startDate && endDate) {
          filter.sale_date = {
            $gte: dayjs(startDate).startOf("day").format("YYYY-MM-DD HH:mm:ss"),
            $lte: dayjs(endDate).endOf("day").format("YYYY-MM-DD HH:mm:ss"),
          };
        } else if (month) {
          filter.sale_date = {
            $gte: dayjs(month).startOf("month").format("YYYY-MM-DD HH:mm:ss"),
            $lte: dayjs(month).endOf("month").format("YYYY-MM-DD HH:mm:ss"),
          };
        }

        // Add product filter
        filter["products.product_name"] = productName;

        const reports = await salesCollection
          .aggregate([
            { $match: filter },
            { $unwind: "$products" },
            { $match: { "products.product_name": productName } },
            {
              $group: {
                _id: {
                  so: "$so",
                  userID: "$user",
                  outlet: "$outlet",
                },
                total_quantity: { $sum: "$products.quantity" },
                total_tp: { $sum: "$products.tp" },
                total_mrp: { $sum: "$products.mrp" },
                total_dp: { $sum: "$products.dp" },
              },
            },
            { $sort: { _id: 1 } },
          ])
          .toArray();

        res.status(200).json(reports);
      } catch (error) {
        console.error("Error fetching outlet details:", error);
        res
          .status(500)
          .json({ message: "Error fetching outlet details", error });
      }
    });

    app.get("/top-products", async (req, res) => {
      try {
        const { month, startDate, endDate } = req.query;
        let filter = {};

        if (startDate && endDate) {
          filter.sale_date = {
            $gte: dayjs(startDate, "YYYY-MM-DD")
              .startOf("day")
              .format("YYYY-MM-DD HH:mm:ss"),
            $lte: dayjs(endDate, "YYYY-MM-DD")
              .endOf("day")
              .format("YYYY-MM-DD HH:mm:ss"),
          };
        } else if (month) {
          filter.sale_date = {
            $gte: dayjs(month, "YYYY-MM")
              .startOf("month")
              .format("YYYY-MM-DD HH:mm:ss"),
            $lte: dayjs(month, "YYYY-MM")
              .endOf("month")
              .format("YYYY-MM-DD HH:mm:ss"),
          };
        } else {
          return res
            .status(400)
            .json({ message: "Please provide a valid month or date range" });
        }

        const topProducts = await salesCollection
          .aggregate([
            { $match: filter },
            { $unwind: "$products" },
            {
              $group: {
                _id: "$products.product_name",
                barcode: { $first: "$products.barcode" },
                total_quantity: { $sum: "$products.quantity" },
                total_tp: { $sum: "$products.tp" },
              },
            },
            { $sort: { total_tp: -1 } },
            { $limit: 10 },
          ])
          .toArray();

        res.status(200).json(topProducts);
      } catch (error) {
        console.error("Error fetching top products:", error);
        res.status(500).json({ message: "Error fetching top products", error });
      }
    });

    app.get("/top-categories", async (req, res) => {
      try {
        const { month, startDate, endDate } = req.query;
        let filter = {};

        if (startDate && endDate) {
          filter.sale_date = {
            $gte: dayjs(startDate, "YYYY-MM-DD")
              .startOf("day")
              .format("YYYY-MM-DD HH:mm:ss"),
            $lte: dayjs(endDate, "YYYY-MM-DD")
              .endOf("day")
              .format("YYYY-MM-DD HH:mm:ss"),
          };
        } else if (month) {
          filter.sale_date = {
            $gte: dayjs(month, "YYYY-MM")
              .startOf("month")
              .format("YYYY-MM-DD HH:mm:ss"),
            $lte: dayjs(month, "YYYY-MM")
              .endOf("month")
              .format("YYYY-MM-DD HH:mm:ss"),
          };
        } else {
          return res
            .status(400)
            .json({ message: "Please provide a valid month or date range" });
        }

        const topCategories = await salesCollection
          .aggregate([
            { $match: filter },
            { $unwind: "$products" },
            {
              $group: {
                _id: "$products.category",
                total_quantity: { $sum: "$products.quantity" },
                total_tp: { $sum: "$products.tp" },
              },
            },
            { $sort: { total_tp: -1 } },
            { $limit: 10 },
          ])
          .toArray();

        res.status(200).json(topCategories);
      } catch (error) {
        console.error("Error fetching top categories:", error);
        res
          .status(500)
          .json({ message: "Error fetching top categories", error });
      }
    });

    app.get("/top-dealers", async (req, res) => {
      const { month } = req.query; // e.g. '2025-03'

      try {
        const start = dayjs(month)
          .startOf("month")
          .format("YYYY-MM-DD HH:mm:ss");
        const end = dayjs(month).endOf("month").format("YYYY-MM-DD HH:mm:ss");

        // Step 1: Aggregate total TP per user for the month
        const topDealers = await salesCollection
          .aggregate([
            {
              $match: {
                sale_date: { $gte: start, $lte: end },
              },
            },
            {
              $group: {
                _id: "$user", // this is a string user ID
                total_tp: { $sum: "$total_tp" },
              },
            },
            { $sort: { total_tp: -1 } },
            { $limit: 10 },
          ])
          .toArray();

        // Step 2: Convert string IDs to ObjectIds
        const userIds = topDealers.map((dealer) => dealer._id);
        const usersList = await users.find({ _id: { $in: userIds } }).toArray();

        // Step 3: Merge user names into result
        const result = topDealers.map((dealer) => {
          const user = usersList.find((u) => u._id.toString() === dealer._id); // dealer._id is still a string
          return {
            _id: user?.name || "Unknown Dealer",
            total_tp: dealer.total_tp,
          };
        });

        res.json(result);
      } catch (err) {
        console.error("Error in top-dealers API:", err);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    // Get Stock Transactions (Filter by Type, Date, Outlet)
    app.get("/stock-transactions", async (req, res) => {
      try {
        const { type, startDate, endDate, outlet } = req.query;
        const filter = {};

        if (type) filter.type = type;
        if (outlet) filter.outlet = outlet;
        if (startDate && endDate) {
          filter.date = { $gte: startDate, $lte: endDate };
        }

        const transactions = await stock_transactions.find(filter).toArray();
        res.json(transactions);
      } catch (error) {
        console.error("Error fetching stock transactions:", error);
        res
          .status(500)
          .json({ success: false, message: "Failed to fetch transactions." });
      }
    });

    // Record Stock Transaction
    app.post("/stock-transactions", async (req, res) => {
      try {
        const {
          barcode,
          outlet,
          type,
          quantity,
          date,
          user,
          userID,
          dp,
          tp,
          asm,
          rsm,
          som,
          zone,
          transfered_to,
          transfered_from,
        } = req.body;

        if (!barcode || !outlet || !type || !quantity || !date || !user) {
          return res
            .status(400)
            .json({ success: false, message: "Missing required fields." });
        }

        await stock_transactions.insertOne({
          barcode,
          outlet,
          type,
          asm,
          rsm,
          som,
          zone,
          quantity,
          date,
          user,
          userID,
          dp,
          tp,
          transfered_to,
          transfered_from,
        });

        res.json({
          success: true,
          message: "Stock transaction recorded successfully.",
        });
      } catch (error) {
        console.error("Error recording stock transaction:", error);
        res
          .status(500)
          .json({ success: false, message: "Failed to record transaction." });
      }
    });

    // Record Stock Transaction
    app.post("/money-transfer", async (req, res) => {
      try {
        const {
          outlet,
          userId,
          SO,
          amount,
          asm,
          rsm,
          zone,
          type,
          paymentMode,
          bank,
          imageUrl,
          date,
          createdBy,
        } = req.body;

        await moneyTransactions.insertOne({
          amount,
          outlet,
          userId,
          SO,
          type,
          asm,
          rsm,
          zone,
          date,
          createdBy,
          paymentMode,
          bank,
          imageUrl,
        });

        res.json({
          success: true,
          message: "Stock transaction recorded successfully.",
        });
      } catch (error) {
        console.error("Error recording stock transaction:", error);
        res
          .status(500)
          .json({ success: false, message: "Failed to record transaction." });
      }
    });
    // Get calculated opening stock for any date range
    // app.get("/api/opening-stocks", async (req, res) => {
    //   try {
    //     const { outlet, beforeDate } = req.query;

    //     if (!outlet || !beforeDate) {
    //       return res.status(400).json({
    //         success: false,
    //         message: "Outlet and beforeDate parameters are required",
    //       });
    //     }

    //     // 1. Get fixed opening stock values
    //     const fixedStocks = await outletStockCollection.find({}).toArray();
    //     const fixedOpeningMap = fixedStocks.reduce((acc, stock) => {
    //       acc[stock.barcode] = stock.outlet_stocks[outlet] || {
    //         openingStock: 0,
    //         openingStockValueDP: 0,
    //         openingStockValueTP: 0,
    //       };
    //       return acc;
    //     }, {});

    //     // 2. Get all transactions before the specified date
    //     const transactions = await stock_transactions
    //       .find({
    //         outlet: outlet,
    //         date: { $lt: beforeDate },
    //       })
    //       .toArray();

    //     // 3. Calculate net changes and track last known prices
    //     const netChanges = {};
    //     const lastPrices = {}; // Track last known DP/TP prices

    //     transactions.forEach((t) => {
    //       if (!netChanges[t.barcode]) {
    //         netChanges[t.barcode] = 0;
    //         lastPrices[t.barcode] = { dp: 0, tp: 0 };
    //       }

    //       // Update last known prices if present in transaction
    //       if (t.dp) lastPrices[t.barcode].dp = t.dp;
    //       if (t.tp) lastPrices[t.barcode].tp = t.tp;

    //       switch (t.type.toLowerCase()) {
    //         case "primary":
    //         case "market return":
    //           netChanges[t.barcode] += t.quantity;
    //           break;
    //         case "secondary":
    //         case "office return":
    //           netChanges[t.barcode] -= t.quantity;
    //           break;
    //       }
    //     });

    //     // 4. Calculate actual opening stocks with correct prices
    //     const result = Object.keys(fixedOpeningMap).map((barcode) => {
    //       const fixed = fixedOpeningMap[barcode];
    //       const change = netChanges[barcode] || 0;
    //       const prices = lastPrices[barcode] || { dp: 0, tp: 0 };

    //       // Use last known prices if available, otherwise calculate from fixed opening
    //       const dpPrice =
    //         prices.dp ||
    //         (fixed.openingStock > 0
    //           ? fixed.openingStockValueDP / fixed.openingStock
    //           : 0);
    //       const tpPrice =
    //         prices.tp ||
    //         (fixed.openingStock > 0
    //           ? fixed.openingStockValueTP / fixed.openingStock
    //           : 0);

    //       return {
    //         barcode,
    //         openingStock: fixed.openingStock + change,
    //         openingValueDP: (fixed.openingStock + change) * dpPrice,
    //         openingValueTP: (fixed.openingStock + change) * tpPrice,
    //         dpPrice, // Include the actual price used
    //         tpPrice, // Include the actual price used
    //       };
    //     });

    //     res.json({ success: true, data: result });
    //   } catch (error) {
    //     console.error("Opening stock error:", error);
    //     res.status(500).json({
    //       success: false,
    //       message: "Error calculating opening stocks",
    //     });
    //   }
    // });

    app.get("/api/opening-stocks", async (req, res) => {
      try {
        const { outlet, beforeDate } = req.query;

        if (!outlet || !beforeDate) {
          return res.status(400).json({
            success: false,
            message: "Outlet and beforeDate parameters are required",
          });
        }

        // 1. Get fixed opening stock values
        const fixedStocks = await outletStockCollection.find({}).toArray();
        const fixedOpeningMap = fixedStocks.reduce((acc, stock) => {
          acc[stock.barcode] = stock.outlet_stocks[outlet] || {
            openingStock: 0,
            openingStockValueDP: 0,
            openingStockValueTP: 0,
          };
          return acc;
        }, {});

        // 2. Get all transactions before the specified date
        const transactions = await stock_transactions
          .find({
            outlet: outlet,
            date: { $lt: beforeDate },
          })
          .toArray();

        // 3. Calculate net changes in both quantity and value
        const netChanges = {
          quantity: {},
          valueDP: {},
          valueTP: {},
        };

        transactions.forEach((t) => {
          if (!netChanges.quantity[t.barcode]) {
            netChanges.quantity[t.barcode] = 0;
            netChanges.valueDP[t.barcode] = 0;
            netChanges.valueTP[t.barcode] = 0;
          }

          // Calculate value changes based on transaction type
          switch (t.type.toLowerCase()) {
            case "primary":
            case "market return":
              netChanges.quantity[t.barcode] += t.quantity;
              netChanges.valueDP[t.barcode] += t.quantity * t.dp;
              netChanges.valueTP[t.barcode] += t.quantity * t.tp;
              break;
            case "secondary":
            case "office return":
              netChanges.quantity[t.barcode] -= t.quantity;
              netChanges.valueDP[t.barcode] -= t.quantity * t.dp;
              netChanges.valueTP[t.barcode] -= t.quantity * t.tp;
              break;
          }
        });

        // 4. Calculate final opening stocks with direct values
        const result = Object.keys(fixedOpeningMap).map((barcode) => {
          const fixed = fixedOpeningMap[barcode];
          const quantityChange = netChanges.quantity[barcode] || 0;
          const valueDPChange = netChanges.valueDP[barcode] || 0;
          const valueTPChange = netChanges.valueTP[barcode] || 0;

          return {
            barcode,
            openingStock: fixed.openingStock + quantityChange,
            openingValueDP: fixed.openingStockValueDP + valueDPChange,
            openingValueTP: fixed.openingStockValueTP + valueTPChange,
            dpPrice:
              fixed.openingStock > 0
                ? fixed.openingStockValueDP / fixed.openingStock
                : 0,
            tpPrice:
              fixed.openingStock > 0
                ? fixed.openingStockValueTP / fixed.openingStock
                : 0,
          };
        });

        res.json({ success: true, data: result });
      } catch (error) {
        console.error("Opening stock error:", error);
        res.status(500).json({
          success: false,
          message: "Error calculating opening stocks",
          error: error.message,
        });
      }
    });

    app.get("/api/stock-movement", async (req, res) => {
      try {
        const { outlet, startDate, endDate } = req.query;

        if (!outlet || !startDate || !endDate) {
          return res.status(400).json({
            success: false,
            message: "Outlet, startDate and endDate are required",
          });
        }

        // Get opening stocks
        const openingResponse = await axios.get(
          `http://localhost:5000//api/opening-stocks`,
          { params: { outlet, beforeDate: startDate } },
        );

        if (!openingResponse.data?.success) {
          return res.status(400).json({
            success: false,
            message: "Failed to get opening stock data",
          });
        }

        const openingStocks = openingResponse.data.data;

        // Get transactions for the period
        const transactions = await stock_transactions
          .find({
            outlet: outlet,
            date: { $gte: startDate, $lte: endDate },
          })
          .toArray();

        // Get product info
        const barcodes = [
          ...new Set([
            ...openingStocks.map((s) => s.barcode),
            ...transactions.map((t) => t.barcode),
          ]),
        ];

        const productsCollection = await products
          .find({ barcode: { $in: barcodes } })
          .toArray();

        const productMap = productsCollection.reduce((map, product) => {
          map[product.barcode] = product;
          return map;
        }, {});

        // Calculate movement during period with actual prices and transfer details
        const movement = {};
        transactions.forEach((t) => {
          if (!movement[t.barcode]) {
            movement[t.barcode] = {
              primary: {
                qty: 0,
                dp: 0,
                tp: 0,
                stocks: [],
              },
              secondary: {
                qty: 0,
                dp: 0,
                tp: 0,
                sales: [],
              },
              return: {
                qty: 0,
                dp: 0,
                tp: 0,
                returns: [],
              },
              marketReturn: {
                qty: 0,
                dp: 0,
                tp: 0,
                transfers: [],
              },
              officeReturn: {
                qty: 0,
                dp: 0,
                tp: 0,
                transfers: [],
              },
            };
          }

          const typeKey = t.type.toLowerCase().replace(" ", "");
          const dp = t.dp || productMap[t.barcode]?.dp || 0;
          const tp = t.tp || productMap[t.barcode]?.tp || 0;

          if (typeKey === "primary") {
            movement[t.barcode].primary.qty += t.quantity;
            movement[t.barcode].primary.dp += t.quantity * dp;
            movement[t.barcode].primary.tp += t.quantity * tp;
            movement[t.barcode].primary.stocks.push({
              productName: productMap[t.barcode].name || "Unknown",
              quantity: t.quantity,
              date: t.date,
              invoice: t.invoiceNo || "N/A",
            });
          } else if (typeKey === "secondary") {
            movement[t.barcode].secondary.qty += t.quantity;
            movement[t.barcode].secondary.dp += t.quantity * dp;
            movement[t.barcode].secondary.tp += t.quantity * tp;
            movement[t.barcode].secondary.sales.push({
              partyName: t.partyName || "Unknown",
              quantity: t.quantity,
              date: t.date,
              invoice: t.invoiceNo || "N/A",
            });
          } else if (typeKey === "return") {
            movement[t.barcode].return.qty += t.quantity;
            movement[t.barcode].return.dp += t.quantity * dp;
            movement[t.barcode].return.tp += t.quantity * tp;
            movement[t.barcode].return.returns.push({
              partyName: t.partyName || "Unknown",
              quantity: t.quantity,
              date: t.date,
              invoice: t.invoiceNo || "N/A",
            });
          } else if (typeKey === "marketreturn") {
            movement[t.barcode].marketReturn.qty += t.quantity;
            movement[t.barcode].marketReturn.dp += t.quantity * dp;
            movement[t.barcode].marketReturn.tp += t.quantity * tp;
            movement[t.barcode].marketReturn.transfers.push({
              source: t.transfered_from || "Unknown",
              quantity: t.quantity,
              date: t.date,
            });
          } else if (typeKey === "officereturn") {
            movement[t.barcode].officeReturn.qty += t.quantity;
            movement[t.barcode].officeReturn.dp += t.quantity * dp;
            movement[t.barcode].officeReturn.tp += t.quantity * tp;
            movement[t.barcode].officeReturn.transfers.push({
              destination: t.transfered_to || "Unknown",
              quantity: t.quantity,
              date: t.date,
            });
          }
        });

        // Prepare final report
        const allBarcodes = new Set([
          ...openingStocks.map((s) => s.barcode),
          ...transactions.map((t) => t.barcode),
        ]);

        const reportData = Array.from(allBarcodes)
          .map((barcode) => {
            const opening = openingStocks.find(
              (s) => s.barcode === barcode,
            ) || {
              openingStock: 0,
              dpPrice: 0,
              tpPrice: 0,
            };
            const product = productMap[barcode] || {};
            const mov = movement[barcode] || {
              primary: { qty: 0, dp: 0, tp: 0, stocks: [] },
              secondary: { qty: 0, dp: 0, tp: 0, sales: [] },
              return: { qty: 0, dp: 0, tp: 0, returns: [] },
              marketReturn: { qty: 0, dp: 0, tp: 0, transfers: [] },
              officeReturn: { qty: 0, dp: 0, tp: 0, transfers: [] },
            };

            const closingQty =
              opening.openingStock +
              mov.primary.qty +
              mov.return.qty +
              mov.marketReturn.qty -
              mov.secondary.qty -
              mov.officeReturn.qty;

            return {
              barcode,
              productName: product.name || "Unknown",
              category: product.category || barcode,
              brand: product.brand || "Unbranded",
              openingStock: opening.openingStock,
              priceDP: opening.dpPrice || product.dp || 0,
              priceTP: opening.tpPrice || product.tp || 0,
              primary: mov.primary.qty,
              primaryValueDP: mov.primary.dp,
              primaryValueTP: mov.primary.tp,
              primaryStocks: mov.primary.stocks,
              secondary: mov.secondary.qty,
              secondaryValueDP: mov.secondary.dp,
              secondaryValueTP: mov.secondary.tp,
              secondarySales: mov.secondary.sales,
              return: mov.return.qty,
              returnValueDP: mov.return.dp,
              returnValueTP: mov.return.tp,
              returnReturns: mov.return.returns,
              marketReturn: mov.marketReturn.qty,
              marketReturnValueDP: mov.marketReturn.dp,
              marketReturnValueTP: mov.marketReturn.tp,
              marketReturnTransfers: mov.marketReturn.transfers,
              officeReturn: mov.officeReturn.qty,
              officeReturnValueDP: mov.officeReturn.dp,
              officeReturnValueTP: mov.officeReturn.tp,
              officeReturnTransfers: mov.officeReturn.transfers,
              closingStock: closingQty,
              closingValueDP: closingQty * (opening.dpPrice || product.dp || 0),
              closingValueTP: closingQty * (opening.tpPrice || product.tp || 0),
            };
          })
          .sort((a, b) => a.productName.localeCompare(b.productName));

        res.json({ success: true, data: reportData });
      } catch (error) {
        console.error("Report error:", error);
        res.status(500).json({
          success: false,
          message: "Error generating report",
        });
      }
    });

    app.get("/api/stock-movement-user", async (req, res) => {
      try {
        const { userID, startDate, endDate } = req.query;
        if (!userID || !startDate || !endDate) {
          return res.status(400).json({
            success: false,
            message: "userID, startDate and endDate are required",
          });
        }
        // Get transactions for the period
        const transactions = await stock_transactions
          .find({
            userID: userID,
            date: { $gte: startDate, $lte: endDate },
          })
          .toArray();
        // Get product info
        const barcodes = [...new Set([...transactions.map((t) => t.barcode)])];
        const productsCollection = await products
          .find({ barcode: { $in: barcodes } })
          .toArray();
        const productMap = productsCollection.reduce((map, product) => {
          map[product.barcode] = product;
          return map;
        }, {});
        // Calculate movement during period with actual prices and transfer details
        const movement = {};
        transactions.forEach((t) => {
          if (!movement[t.barcode]) {
            movement[t.barcode] = {
              primary: {
                qty: 0,
                dp: 0,
                tp: 0,
                stocks: [],
              },
              secondary: {
                qty: 0,
                dp: 0,
                tp: 0,
                sales: [],
              },
              return: {
                qty: 0,
                dp: 0,
                tp: 0,
                returns: [],
              },
              marketReturn: {
                qty: 0,
                dp: 0,
                tp: 0,
                transfers: [],
              },
              officeReturn: {
                qty: 0,
                dp: 0,
                tp: 0,
                transfers: [],
              },
            };
          }
          const typeKey = t.type.toLowerCase().replace(" ", "");
          const dp = t.dp || productMap[t.barcode]?.dp || 0;
          const tp = t.tp || productMap[t.barcode]?.tp || 0;
          if (typeKey === "primary") {
            movement[t.barcode].primary.qty += t.quantity;
            movement[t.barcode].primary.dp += t.quantity * dp;
            movement[t.barcode].primary.tp += t.quantity * tp;
            movement[t.barcode].primary.stocks.push({
              productName: productMap[t.barcode].name || "Unknown",
              quantity: t.quantity,
              date: t.date,
              invoice: t.invoiceNo || "N/A",
            });
          } else if (typeKey === "secondary") {
            movement[t.barcode].secondary.qty += t.quantity;
            movement[t.barcode].secondary.dp += t.quantity * dp;
            movement[t.barcode].secondary.tp += t.quantity * tp;
            movement[t.barcode].secondary.sales.push({
              partyName: t.partyName || "Unknown",
              quantity: t.quantity,
              date: t.date,
              invoice: t.invoiceNo || "N/A",
            });
          } else if (typeKey === "return") {
            movement[t.barcode].return.qty += t.quantity;
            movement[t.barcode].return.dp += t.quantity * dp;
            movement[t.barcode].return.tp += t.quantity * tp;
            movement[t.barcode].return.returns.push({
              partyName: t.partyName || "Unknown",
              quantity: t.quantity,
              date: t.date,
              invoice: t.invoiceNo || "N/A",
            });
          } else if (typeKey === "marketreturn") {
            movement[t.barcode].marketReturn.qty += t.quantity;
            movement[t.barcode].marketReturn.dp += t.quantity * dp;
            movement[t.barcode].marketReturn.tp += t.quantity * tp;
            movement[t.barcode].marketReturn.transfers.push({
              source: t.transfered_from || "Unknown",
              quantity: t.quantity,
              date: t.date,
            });
          } else if (typeKey === "officereturn") {
            movement[t.barcode].officeReturn.qty += t.quantity;
            movement[t.barcode].officeReturn.dp += t.quantity * dp;
            movement[t.barcode].officeReturn.tp += t.quantity * tp;
            movement[t.barcode].officeReturn.transfers.push({
              destination: t.transfered_to || "Unknown",
              quantity: t.quantity,
              date: t.date,
            });
          }
        });
        // Prepare final report
        const allBarcodes = new Set([...transactions.map((t) => t.barcode)]);
        const reportData = Array.from(allBarcodes)
          .map((barcode) => {
            const product = productMap[barcode] || {};
            const mov = movement[barcode] || {
              primary: { qty: 0, dp: 0, tp: 0, stocks: [] },
              secondary: { qty: 0, dp: 0, tp: 0, sales: [] },
              return: { qty: 0, dp: 0, tp: 0, returns: [] },
              marketReturn: { qty: 0, dp: 0, tp: 0, transfers: [] },
              officeReturn: { qty: 0, dp: 0, tp: 0, transfers: [] },
            };
            return {
              barcode,
              productName: product.name || "Unknown",
              category: product.category || barcode,
              brand: product.brand || "Unbranded",
              priceDP: product.dp || 0,
              priceTP: product.tp || 0,
              primary: mov.primary.qty,
              primaryValueDP: mov.primary.dp,
              primaryValueTP: mov.primary.tp,
              primaryStocks: mov.primary.stocks,
              secondary: mov.secondary.qty,
              secondaryValueDP: mov.secondary.dp,
              secondaryValueTP: mov.secondary.tp,
              secondarySales: mov.secondary.sales,
              return: mov.return.qty,
              returnValueDP: mov.return.dp,
              returnValueTP: mov.return.tp,
              returnReturns: mov.return.returns,
              marketReturn: mov.marketReturn.qty,
              marketReturnValueDP: mov.marketReturn.dp,
              marketReturnValueTP: mov.marketReturn.tp,
              marketReturnTransfers: mov.marketReturn.transfers,
              officeReturn: mov.officeReturn.qty,
              officeReturnValueDP: mov.officeReturn.dp,
              officeReturnValueTP: mov.officeReturn.tp,
              officeReturnTransfers: mov.officeReturn.transfers,
            };
          })
          .sort((a, b) => a.productName.localeCompare(b.productName));
        res.json({ success: true, data: reportData });
      } catch (error) {
        console.error("Report error:", error);
        res.status(500).json({
          success: false,
          message: "Error generating report",
        });
      }
    });
    // GET /detailed-stock-transactions
    app.get("/detailed-stock-transactions", async (req, res) => {
      const { outlet, startDate, endDate, type } = req.query;
      const query = { outlet };
      if (startDate && endDate) {
        query.date = { $gte: startDate, $lte: endDate };
      }
      if (type) query.type = type;

      try {
        const transactions = await stock_transactions.find(query).toArray();
        // Fetch product names for each transaction
        const transactionsWithProductNames = await Promise.all(
          transactions.map(async (txn) => {
            const product = await products.findOne(
              { barcode: txn.barcode },
              { projection: { name: 1 } },
            );
            return {
              ...txn,
              productName: product?.name || "Unknown Product",
            };
          }),
        );
        res.json({ success: true, data: transactionsWithProductNames });
      } catch (error) {
        console.error("Error fetching transactions:", error);
        res
          .status(500)
          .json({ success: false, message: "Failed to fetch transactions" });
      }
    });

    // // PUT /stock-transaction/:id
    // app.put("/stock-transaction/:id", async (req, res) => {
    //   const { id } = req.params;
    //   const { quantity, type, date } = req.body;

    //   // Validate input
    //   if (!quantity || !type || !date) {
    //     return res.status(400).json({
    //       success: false,
    //       message: "Quantity, type, and date are required",
    //     });
    //   }
    //   // if (!["primary", "market return", "office return"].includes(type)) {
    //   //   return res.status(400).json({
    //   //     success: false,
    //   //     message:
    //   //       "Only Primary, Market Return, and Office Return transactions can be updated",
    //   //   });
    //   // }

    //   try {
    //     const txn = await stock_transactions.findOne({ _id: new ObjectId(id) });
    //     if (!txn) {
    //       return res
    //         .status(404)
    //         .json({ success: false, message: "Transaction not found" });
    //     }
    //     // if (!["primary", "market return", "office return"].includes(txn.type)) {
    //     //   return res.status(400).json({
    //     //     success: false,
    //     //     message:
    //     //       "Only Primary, Market Return, and Office Return transactions can be updated",
    //     //   });
    //     // }

    //     // Check if type is changing and if there are multiple items
    //     if (type !== txn.type) {
    //       const count = await stock_transactions.countDocuments({
    //         transaction_id: txn.transaction_id,
    //       });
    //       if (count > 1) {
    //         return res.status(400).json({
    //           success: false,
    //           message: "Cannot change type for multi-item voucher",
    //         });
    //       }
    //     }

    //     // Reverse old effect on stock
    //     const oldSign = getStockAdjustSign(txn.type);
    //     const oldAdjustQty = oldSign * txn.quantity;

    //     let updateOp = {
    //       $inc: {
    //         [`outlet_stocks.${txn.outlet}.currentStock`]: -oldAdjustQty,
    //       },
    //     };

    //     if (txn.type === "opening") {
    //       if (!updateOp.$set) updateOp.$set = {};
    //       updateOp.$set[`outlet_stocks.${txn.outlet}.openingStock`] = 0;
    //     }

    //     await outletStockCollection.updateOne(
    //       { barcode: txn.barcode },
    //       updateOp
    //     );

    //     // Apply new effect on stock
    //     const newQuantity = parseFloat(quantity);
    //     const newType = type;
    //     const newSign = getStockAdjustSign(newType);
    //     const newAdjustQty = newSign * newQuantity;

    //     updateOp = {
    //       $inc: {
    //         [`outlet_stocks.${txn.outlet}.currentStock`]: newAdjustQty,
    //       },
    //     };

    //     if (newType === "opening") {
    //       if (!updateOp.$set) updateOp.$set = {};
    //       updateOp.$set[`outlet_stocks.${txn.outlet}.openingStock`] =
    //         newQuantity;
    //     }

    //     await outletStockCollection.updateOne(
    //       { barcode: txn.barcode },
    //       updateOp
    //     );

    //     // Update stock transaction
    //     await stock_transactions.updateOne(
    //       { _id: new ObjectId(id) },
    //       {
    //         $set: {
    //           quantity: newQuantity,
    //           type: newType,
    //           date: date,
    //         },
    //       }
    //     );

    //     res.json({
    //       success: true,
    //       message: "Transaction updated successfully",
    //     });
    //   } catch (error) {
    //     console.error("Error updating stock transaction:", error);
    //     res
    //       .status(500)
    //       .json({ success: false, message: "Failed to update transaction" });
    //   }
    // });
    // // DELETE /stock-transaction/:id
    // app.delete("/stock-transaction/:id", async (req, res) => {
    //   const { id } = req.params;
    //   try {
    //     const txn = await stock_transactions.findOne({ _id: new ObjectId(id) });
    //     if (!txn) {
    //       return res
    //         .status(404)
    //         .json({ success: false, message: "Transaction not found" });
    //     }

    //     // Reverse effect on stock
    //     const sign = getStockAdjustSign(txn.type);
    //     const adjustQty = sign * txn.quantity;

    //     let updateOp = {
    //       $inc: {
    //         [`outlet_stocks.${txn.outlet}.currentStock`]: -adjustQty,
    //       },
    //     };

    //     if (txn.type === "opening") {
    //       if (!updateOp.$set) updateOp.$set = {};
    //       updateOp.$set[`outlet_stocks.${txn.outlet}.openingStock`] = 0;
    //     }

    //     await outletStockCollection.updateOne(
    //       { barcode: txn.barcode },
    //       updateOp
    //     );

    //     // Delete the stock transaction
    //     await stock_transactions.deleteOne({ _id: new ObjectId(id) });

    //     res.json({
    //       success: true,
    //       message: "Transaction deleted successfully",
    //     });
    //   } catch (error) {
    //     console.error("Error deleting stock transaction:", error);
    //     res
    //       .status(500)
    //       .json({ success: false, message: "Failed to delete transaction" });
    //   }
    // });

    app.put("/stock-transactions/invoice/:invoiceNo", async (req, res) => {
      const { invoiceNo } = req.params;
      const { date, type, partyName, items } = req.body; // items: [{ _id, quantity }]

      // Validate input
      // if (!date || !type || !items || !Array.isArray(items)) {
      //   return res.status(400).json({
      //     success: false,
      //     message: "Date, type, partyName, and items are required",
      //   });
      // }

      try {
        const transactions = await stock_transactions
          .find({ invoiceNo })
          .toArray();
        if (transactions.length === 0) {
          return res.status(404).json({
            success: false,
            message: "No transactions found for this invoice",
          });
        }

        const itemMap = new Map(items.map((it) => [it._id, it.quantity]));

        // Handle removed items
        for (let txn of transactions) {
          if (!itemMap.has(txn._id.toString())) {
            // Reverse stock effect for removed item
            const sign = getStockAdjustSign(txn.type);
            const adjustQty = sign * txn.quantity;
            let updateOp = {
              $inc: {
                [`outlet_stocks.${txn.outlet}.currentStock`]: -adjustQty,
              },
            };
            if (txn.type === "opening") {
              updateOp.$set = {
                [`outlet_stocks.${txn.outlet}.openingStock`]: 0,
              };
            }
            await outletStockCollection.updateOne(
              { barcode: txn.barcode },
              updateOp,
            );
            // Delete the transaction
            await stock_transactions.deleteOne({ _id: txn._id });
          }
        }

        // Update remaining items
        for (let txn of transactions) {
          const newQuantity = itemMap.get(txn._id.toString());
          if (newQuantity === undefined) continue; // Already handled as removed

          // Reverse old effect
          const oldSign = getStockAdjustSign(txn.type);
          const oldAdjustQty = oldSign * txn.quantity;
          let updateOp = {
            $inc: {
              [`outlet_stocks.${txn.outlet}.currentStock`]: -oldAdjustQty,
            },
          };
          if (txn.type === "opening") {
            if (!updateOp.$set) updateOp.$set = {};
            updateOp.$set[`outlet_stocks.${txn.outlet}.openingStock`] = 0;
          }
          await outletStockCollection.updateOne(
            { barcode: txn.barcode },
            updateOp,
          );

          // Apply new effect
          const newSign = getStockAdjustSign(type);
          const newAdjustQty = newSign * newQuantity;
          updateOp = {
            $inc: {
              [`outlet_stocks.${txn.outlet}.currentStock`]: newAdjustQty,
            },
          };
          if (type === "opening") {
            if (!updateOp.$set) updateOp.$set = {};
            updateOp.$set[`outlet_stocks.${txn.outlet}.openingStock`] =
              newQuantity;
          }
          await outletStockCollection.updateOne(
            { barcode: txn.barcode },
            updateOp,
          );

          // Update transaction
          await stock_transactions.updateOne(
            { _id: txn._id },
            {
              $set: {
                quantity: newQuantity,
                type,
                date: date,
                partyName,
              },
            },
          );
        }

        res.json({
          success: true,
          message: "Invoice transactions updated successfully",
        });
      } catch (error) {
        console.error("Error updating invoice transactions:", error);
        res
          .status(500)
          .json({ success: false, message: "Failed to update transactions" });
      }
    });

    app.delete("/stock-transactions/invoice/:invoiceNo", async (req, res) => {
      const { invoiceNo } = req.params;
      try {
        const transactions = await stock_transactions
          .find({ invoiceNo })
          .toArray();
        if (transactions.length === 0) {
          return res.status(404).json({
            success: false,
            message: "No transactions found for this invoice",
          });
        }

        for (let txn of transactions) {
          // Reverse stock effect
          const sign = getStockAdjustSign(txn.type);
          const adjustQty = sign * txn.quantity;
          let updateOp = {
            $inc: {
              [`outlet_stocks.${txn.outlet}.currentStock`]: -adjustQty,
            },
          };
          if (txn.type === "opening") {
            updateOp.$set = { [`outlet_stocks.${txn.outlet}.openingStock`]: 0 };
          }
          await outletStockCollection.updateOne(
            { barcode: txn.barcode },
            updateOp,
          );
          // Delete transaction
          await stock_transactions.deleteOne({ _id: txn._id });
        }

        res.json({
          success: true,
          message: "Invoice transactions deleted successfully",
        });
      } catch (error) {
        console.error("Error deleting invoice transactions:", error);
        res
          .status(500)
          .json({ success: false, message: "Failed to delete transactions" });
      }
    });
    function getStockAdjustSign(type) {
      if (type === "primary" || type === "market return" || type === "opening")
        return 1;
      if (type === "secondary" || type === "office return") return -1;
      return 0;
    }
    // Get area options (ASM, RSM, Zone)
    app.get("/api/area-options", async (req, res) => {
      try {
        const { type } = req.query;

        if (!type) {
          return res.status(400).json({
            success: false,
            message: "Type parameter is required (ASM, RSM, or Zone)",
          });
        }

        const fieldMap = {
          ASM: "asm",
          RSM: "rsm",
          Zone: "zone",
        };

        const fieldName = fieldMap[type];
        if (!fieldName) {
          return res.status(400).json({
            success: false,
            message: "Invalid type parameter",
          });
        }

        const options = await stock_transactions.distinct(fieldName);

        res.json({
          success: true,
          data: options.filter((opt) => opt), // Remove null/undefined
        });
      } catch (error) {
        console.error("Area options error:", error);
        res.status(500).json({
          success: false,
          message: "Error fetching area options",
        });
      }
    });

    // Get area stock movement
    app.get("/api/area-stock-movement", async (req, res) => {
      try {
        const { areaType, areaValue, startDate, endDate } = req.query;

        if (!areaType || !areaValue || !startDate || !endDate) {
          return res.status(400).json({
            success: false,
            message: "All parameters are required",
          });
        }

        const fieldMap = {
          ASM: "asm",
          RSM: "rsm",
          Zone: "zone",
        };

        const fieldName = fieldMap[areaType];
        if (!fieldName) {
          return res.status(400).json({
            success: false,
            message: "Invalid areaType parameter",
          });
        }

        // 1. Get all outlets for this area
        const outlets = await stock_transactions.distinct("outlet", {
          [fieldName]: areaValue,
        });

        if (outlets.length === 0) {
          return res.json({ success: true, data: [] });
        }

        // 2. Get opening stocks for all outlets
        const openingPromises = outlets.map((outlet) =>
          axios.get("http://localhost:5000//api/opening-stocks", {
            params: { outlet, beforeDate: startDate },
          }),
        );

        const openingResponses = await Promise.all(openingPromises);
        const allOpeningStocks = openingResponses
          .filter((res) => res.data?.success)
          .flatMap((res) => res.data.data);

        // 3. Get transactions for all outlets in this period
        const transactions = await stock_transactions
          .find({
            [fieldName]: areaValue,
            date: { $gte: startDate, $lte: endDate },
          })
          .toArray();

        // 4. Get product info
        const barcodes = [
          ...new Set([
            ...allOpeningStocks.map((s) => s.barcode),
            ...transactions.map((t) => t.barcode),
          ]),
        ];

        const productsCollection = await products
          .find({ barcode: { $in: barcodes } })
          .toArray();

        const productMap = productsCollection.reduce((map, product) => {
          map[product.barcode] = product;
          return map;
        }, {});

        // 5. Calculate movement during period with actual prices and transfer details
        const movement = {};
        transactions.forEach((t) => {
          if (!movement[t.barcode]) {
            movement[t.barcode] = {
              primary: { qty: 0, dp: 0, tp: 0, stocks: [] },
              secondary: { qty: 0, dp: 0, tp: 0, sales: [] },
              return: { qty: 0, dp: 0, tp: 0, returns: [] },
              marketReturn: { qty: 0, dp: 0, tp: 0, transfers: [] },
              officeReturn: { qty: 0, dp: 0, tp: 0, transfers: [] },
            };
          }

          const typeKey = t.type.toLowerCase().replace(" ", "");
          const dp = t.dp || productMap[t.barcode]?.dp || 0;
          const tp = t.tp || productMap[t.barcode]?.tp || 0;
          const productName = productMap[t.barcode]?.name || "Unknown"; // Get product name for primary stocks

          if (typeKey === "primary") {
            movement[t.barcode].primary.qty += t.quantity;
            movement[t.barcode].primary.dp += t.quantity * dp;
            movement[t.barcode].primary.tp += t.quantity * tp;
            movement[t.barcode].primary.stocks.push({
              productName, // Include productName instead of partyName
              quantity: t.quantity,
              date: t.date,
              invoice: t.invoiceNo || "N/A",
              outlet: t.outlet || "Unknown",
            });
          } else if (typeKey === "secondary") {
            movement[t.barcode].secondary.qty += t.quantity;
            movement[t.barcode].secondary.dp += t.quantity * dp;
            movement[t.barcode].secondary.tp += t.quantity * tp;
            movement[t.barcode].secondary.sales.push({
              partyName: t.partyName || "Unknown",
              quantity: t.quantity,
              date: t.date,
              invoice: t.invoiceNo || "N/A",
              outlet: t.outlet || "Unknown",
            });
          } else if (typeKey === "return") {
            movement[t.barcode].return.qty += t.quantity;
            movement[t.barcode].return.dp += t.quantity * dp;
            movement[t.barcode].return.tp += t.quantity * tp;
            movement[t.barcode].return.returns.push({
              partyName: t.partyName || "Unknown",
              quantity: t.quantity,
              date: t.date,
              invoice: t.invoiceNo || "N/A",
              outlet: t.outlet || "Unknown",
            });
          } else if (typeKey === "marketreturn") {
            movement[t.barcode].marketReturn.qty += t.quantity;
            movement[t.barcode].marketReturn.dp += t.quantity * dp;
            movement[t.barcode].marketReturn.tp += t.quantity * tp;
            movement[t.barcode].marketReturn.transfers.push({
              source: t.transfered_from || "Unknown",
              quantity: t.quantity,
              date: t.date,
              outlet: t.outlet || "Unknown",
            });
          } else if (typeKey === "officereturn") {
            movement[t.barcode].officeReturn.qty += t.quantity;
            movement[t.barcode].officeReturn.dp += t.quantity * dp;
            movement[t.barcode].officeReturn.tp += t.quantity * tp;
            movement[t.barcode].officeReturn.transfers.push({
              destination: t.transfered_to || "Unknown",
              quantity: t.quantity,
              date: t.date,
              outlet: t.outlet || "Unknown",
            });
          }
        });

        // 6. Aggregate opening stocks by product
        const aggregatedOpening = {};
        allOpeningStocks.forEach((stock) => {
          if (!aggregatedOpening[stock.barcode]) {
            aggregatedOpening[stock.barcode] = {
              openingStock: 0,
              openingValueDP: 0,
              openingValueTP: 0,
              dpPrice: 0,
              tpPrice: 0,
            };
          }

          aggregatedOpening[stock.barcode].openingStock +=
            stock.openingStock || 0;
          aggregatedOpening[stock.barcode].openingValueDP +=
            stock.openingValueDP || 0;
          aggregatedOpening[stock.barcode].openingValueTP +=
            stock.openingValueTP || 0;
        });

        // Calculate average prices
        Object.keys(aggregatedOpening).forEach((barcode) => {
          const stock = aggregatedOpening[barcode];
          stock.dpPrice =
            stock.openingStock > 0
              ? stock.openingValueDP / stock.openingStock
              : productMap[barcode]?.dp || 0;
          stock.tpPrice =
            stock.openingStock > 0
              ? stock.openingValueTP / stock.openingStock
              : productMap[barcode]?.tp || 0;
        });

        // 7. Prepare final report with transfer details
        const reportData = Object.keys(aggregatedOpening)
          .map((barcode) => {
            const opening = aggregatedOpening[barcode];
            const product = productMap[barcode] || {};
            const mov = movement[barcode] || {
              primary: { qty: 0, dp: 0, tp: 0, stocks: [] },
              secondary: { qty: 0, dp: 0, tp: 0, sales: [] },
              return: { qty: 0, dp: 0, tp: 0, returns: [] },
              marketReturn: { qty: 0, dp: 0, tp: 0, transfers: [] },
              officeReturn: { qty: 0, dp: 0, tp: 0, transfers: [] },
            };

            const closingQty =
              opening.openingStock +
              mov.primary.qty +
              mov.return.qty +
              mov.marketReturn.qty -
              mov.secondary.qty -
              mov.officeReturn.qty;

            return {
              barcode,
              productName: product.name || "Unknown",
              category: product.category || "Uncategorized",
              brand: product.brand || "Unbranded",
              priceDP: opening.dpPrice,
              priceTP: opening.tpPrice,
              openingStock: opening.openingStock,
              openingValueDP: opening.openingValueDP,
              openingValueTP: opening.openingValueTP,
              primary: mov.primary.qty,
              primaryValueDP: mov.primary.dp,
              primaryValueTP: mov.primary.tp,
              primaryStocks: mov.primary.stocks,
              secondary: mov.secondary.qty,
              secondaryValueDP: mov.secondary.dp,
              secondaryValueTP: mov.secondary.tp,
              secondarySales: mov.secondary.sales,
              return: mov.return.qty,
              returnValueDP: mov.return.dp,
              returnValueTP: mov.return.tp,
              returnReturns: mov.return.returns,
              marketReturn: mov.marketReturn.qty,
              marketReturnValueDP: mov.marketReturn.dp,
              marketReturnValueTP: mov.marketReturn.tp,
              marketReturnTransfers: mov.marketReturn.transfers,
              officeReturn: mov.officeReturn.qty,
              officeReturnValueDP: mov.officeReturn.dp,
              officeReturnValueTP: mov.officeReturn.tp,
              officeReturnTransfers: mov.officeReturn.transfers,
              closingStock: closingQty,
              closingValueDP: closingQty * opening.dpPrice,
              closingValueTP: closingQty * opening.tpPrice,
            };
          })
          .sort((a, b) => a.productName.localeCompare(b.productName));

        res.json({ success: true, data: reportData });
      } catch (error) {
        console.error("Area report error:", error);
        res.status(500).json({
          success: false,
          message: "Error generating area report",
        });
      }
    });

    app.get("/api/total-stock-movement", async (req, res) => {
      try {
        const { startDate, endDate } = req.query;

        if (!startDate || !endDate) {
          return res.status(400).json({
            success: false,
            message: "startDate and endDate are required",
          });
        }

        // 1. Get all outlets
        const outletsResponse = await axios.get(
          "http://localhost:5000//get-outlets",
        );
        const outlets = outletsResponse.data;

        // 2. Get product list for reference
        const productsCollection = await products.find({}).toArray();
        const productMap = productsCollection.reduce((map, product) => {
          map[product.barcode] = product;
          return map;
        }, {});

        // 3. Initialize aggregated report structure
        const aggregatedReport = {};

        // 4. Process each outlet
        for (const outlet of outlets) {
          const outletName = outlet.name || outlet._id;

          // Get stock movement for this outlet
          const movementResponse = await axios.get(
            "http://localhost:5000//api/stock-movement",
            {
              params: {
                outlet: outletName,
                startDate,
                endDate,
              },
            },
          );

          if (movementResponse.data?.success) {
            const outletData = movementResponse.data.data;

            // Aggregate data for each product
            outletData.forEach((product) => {
              if (!aggregatedReport[product.barcode]) {
                aggregatedReport[product.barcode] = {
                  productName: product.productName,
                  barcode: product.barcode,
                  priceDP: product.priceDP,
                  priceTP: product.priceTP,
                  totalOpeningStock: 0,
                  totalOpeningValueDP: 0,
                  totalOpeningValueTP: 0,
                  totalPrimary: 0,
                  totalPrimaryValueDP: 0,
                  totalPrimaryValueTP: 0,
                  totalSecondary: 0,
                  totalSecondaryValueDP: 0,
                  totalSecondaryValueTP: 0,
                  totalMarketReturn: 0,
                  totalMarketReturnValueDP: 0,
                  totalMarketReturnValueTP: 0,
                  totalOfficeReturn: 0,
                  totalOfficeReturnValueDP: 0,
                  totalOfficeReturnValueTP: 0,
                  totalClosingStock: 0,
                  totalClosingValueDP: 0,
                  totalClosingValueTP: 0,
                  outletDetails: [],
                };
              }

              // Sum up all values
              const aggProduct = aggregatedReport[product.barcode];
              aggProduct.totalOpeningStock += product.openingStock;
              aggProduct.totalOpeningValueDP += product.openingValueDP;
              aggProduct.totalOpeningValueTP += product.openingValueTP;
              aggProduct.totalPrimary += product.primary;
              aggProduct.totalPrimaryValueDP += product.primaryValueDP;
              aggProduct.totalPrimaryValueTP += product.primaryValueTP;
              aggProduct.totalSecondary += product.secondary;
              aggProduct.totalSecondaryValueDP += product.secondaryValueDP;
              aggProduct.totalSecondaryValueTP += product.secondaryValueTP;
              aggProduct.totalMarketReturn += product.marketReturn;
              aggProduct.totalMarketReturnValueDP +=
                product.marketReturnValueDP;
              aggProduct.totalMarketReturnValueTP +=
                product.marketReturnValueTP;
              aggProduct.totalOfficeReturn += product.officeReturn;
              aggProduct.totalOfficeReturnValueDP +=
                product.officeReturnValueDP;
              aggProduct.totalOfficeReturnValueTP +=
                product.officeReturnValueTP;
              aggProduct.totalClosingStock += product.closingStock;
              aggProduct.totalClosingValueDP += product.closingValueDP;
              aggProduct.totalClosingValueTP += product.closingValueTP;

              // Store outlet-specific details
              aggProduct.outletDetails.push({
                outlet: outletName,
                openingStock: product.openingStock,
                closingStock: product.closingStock,
                movement:
                  product.primary -
                  product.secondary +
                  product.marketReturn -
                  product.officeReturn,
              });
            });
          }
        }

        // 5. Convert to array format and calculate percentages if needed
        const result = Object.values(aggregatedReport).map((item) => {
          // Calculate any derived metrics here if needed
          return item;
        });

        res.json({
          success: true,
          data: {
            summary: result,
            totalProducts: result.length,
            totalOpeningValueDP: result.reduce(
              (sum, p) => sum + p.totalOpeningValueDP,
              0,
            ),
            totalClosingValueDP: result.reduce(
              (sum, p) => sum + p.totalClosingValueDP,
              0,
            ),
            totalOpeningValueTP: result.reduce(
              (sum, p) => sum + p.totalOpeningValueTP,
              0,
            ),
            totalClosingValueTP: result.reduce(
              (sum, p) => sum + p.totalClosingValueTP,
              0,
            ),
            periodPrimary: result.reduce((sum, p) => sum + p.totalPrimary, 0),
            periodSecondary: result.reduce(
              (sum, p) => sum + p.totalSecondary,
              0,
            ),
            periodMarketReturn: result.reduce(
              (sum, p) => sum + p.totalMarketReturn,
              0,
            ),
            periodOfficeReturn: result.reduce(
              (sum, p) => sum + p.totalOfficeReturn,
              0,
            ),
            outlets: outlets.map((o) => o.name || o._id),
          },
        });
      } catch (error) {
        console.error("Total stock movement error:", error);
        res.status(500).json({
          success: false,
          message: "Error generating total stock movement report",
          error: error.message,
        });
      }
    });

    // Get stock transactions report (all-user-based)
    // for salary sheet
    app.get("/api/stock-transactions-report", async (req, res) => {
      try {
        const { month, startDate, endDate } = req.query;

        let dateFilter = {};

        if (startDate && endDate) {
          dateFilter.date = {
            $gte: dayjs(startDate, "YYYY-MM-DD").format("YYYY-MM-DD HH:mm:ss"),
            $lte: dayjs(endDate, "YYYY-MM-DD")
              .endOf("day")
              .format("YYYY-MM-DD HH:mm:ss"),
          };
        } else if (month) {
          dateFilter.date = {
            $gte: dayjs(month, "YYYY-MM")
              .startOf("month")
              .format("YYYY-MM-DD HH:mm:ss"),
            $lte: dayjs(month, "YYYY-MM")
              .endOf("month")
              .format("YYYY-MM-DD HH:mm:ss"),
          };
        } else {
          return res.status(400).json({
            success: false,
            message: "Provide month or custom date range",
          });
        }

        // Get all transactions in the period
        const transactions = await stock_transactions
          .find(dateFilter)
          .toArray();

        // Get unique userIDs and handle both string and ObjectId formats
        const userIds = [...new Set(transactions.map((t) => t.userID))]
          .filter((id) => id && id.toString().match(/^[0-9a-fA-F]{24}$/)) // Validate format
          .map((id) => {
            try {
              // If already ObjectId, use as-is, otherwise create new ObjectId
              return id instanceof ObjectId ? id : new ObjectId(id);
            } catch (e) {
              console.error(`Invalid userID format: ${id}`);
              return null;
            }
          })
          .filter((id) => id); // Remove null values

        // Get user info
        const usersCollection = await users
          .find({ _id: { $in: userIds } })
          .toArray();

        // Create lookup map using string representations
        const userMap = usersCollection.reduce((map, user) => {
          map[user._id.toString()] = user;
          return map;
        }, {});

        // Map transactions with user info
        const response = transactions.map((t) => {
          const userIDStr = t.userID?.toString();
          const user = userMap[userIDStr];

          return {
            ...t,
            name: user?.name || "Unknown",
            role: user?.role || "SO",
            asm: user?.asm || "",
            rsm: user?.rsm || "",
            som: user?.som || "",
            zone: user?.zone || "",
          };
        });

        res.status(200).json(response);
      } catch (error) {
        console.error("Error fetching stock transaction reports:", error);
        res.status(500).json({
          message: "Error fetching stock transactions report",
          error: error.message,
        });
      }
    });
    // for salary sheet
    app.get("/api/user-stock-movement", async (req, res) => {
      const formatDate = (d, end = false) =>
        dayjs(d).format(end ? "YYYY-MM-DD 23:59:59" : "YYYY-MM-DD 00:00:00");

      const getOpeningStocks = async (outletName, beforeDateISO) => {
        const fixedDocs = await outletStockCollection
          .find(
            { [`outlet_stocks.${outletName}`]: { $exists: true } },
            { projection: { barcode: 1, [`outlet_stocks.${outletName}`]: 1 } },
          )
          .toArray();

        const fixed = Object.fromEntries(
          fixedDocs.map((d) => [d.barcode, d.outlet_stocks[outletName]]),
        );

        const delta = {};
        const cursor = stock_transactions.find(
          { outlet: outletName, date: { $lt: beforeDateISO } },
          { projection: { barcode: 1, quantity: 1, dp: 1, tp: 1, type: 1 } },
        );
        for await (const t of cursor) {
          const b = t.barcode;
          if (!delta[b]) delta[b] = { qty: 0, dp: 0, tp: 0 };
          const m = ["primary", "market return"].includes(t.type.toLowerCase())
            ? 1
            : -1;
          delta[b].qty += m * t.quantity;
          delta[b].dp += m * t.quantity * t.dp;
          delta[b].tp += m * t.quantity * t.tp;
        }

        return Object.entries(fixed).map(([barcode, f]) => {
          const d = delta[barcode] || { qty: 0, dp: 0, tp: 0 };
          return {
            barcode,
            openingStock: (f.openingStock || 0) + d.qty,
            openingValueDP: (f.openingStockValueDP || 0) + d.dp,
            openingValueTP: (f.openingStockValueTP || 0) + d.tp,
          };
        });
      };

      try {
        const { month, startDate, endDate } = req.query;
        if (!(startDate && endDate) && !month) {
          return res.status(400).json({
            success: false,
            message: "Provide either month or startDate/endDate",
          });
        }

        const rangeStart =
          startDate || dayjs(month).startOf("month").format("YYYY-MM-DD");
        const rangeEnd =
          endDate || dayjs(month).endOf("month").format("YYYY-MM-DD");

        const [allOutlets, allUsers] = await Promise.all([
          outlet_collection.find({}).toArray(),
          users.find({}).toArray(),
        ]);
        if (!allOutlets.length) {
          return res
            .status(404)
            .json({ success: false, message: "No outlets found" });
        }
        const userMap = Object.fromEntries(
          allUsers.map((u) => [u._id.toString(), u]),
        );

        const BATCH_SIZE = 200;
        const results = [];

        for (let i = 0; i < allOutlets.length; i += BATCH_SIZE) {
          const batch = allOutlets.slice(i, i + BATCH_SIZE);

          const batchRes = await Promise.all(
            batch.map(async (outlet) => {
              try {
                const opening = await getOpeningStocks(
                  outlet.outlet_name,
                  rangeStart,
                );

                const tx = await stock_transactions
                  .find(
                    {
                      outlet: outlet.outlet_name,
                      date: {
                        $gte: formatDate(rangeStart),
                        $lte: formatDate(rangeEnd, true),
                      },
                    },
                    {
                      projection: {
                        userID: 1,
                        type: 1,
                        quantity: 1,
                        dp: 1,
                        tp: 1,
                      },
                    },
                  )
                  .toArray();

                // Only query collections (using "payment" type as per your requirement)
                const collections = await moneyTransactions
                  .find(
                    {
                      outlet: outlet.outlet_name,
                      type: "payment", // This represents collections in your system
                      date: {
                        $gte: formatDate(rangeStart),
                        $lte: formatDate(rangeEnd, true),
                      },
                    },
                    { projection: { userId: 1, amount: 1 } },
                  )
                  .toArray();

                // console.log(collections);

                const userTotals = {};
                for (const t of tx) {
                  const uid = t.userID?.toString();
                  if (!uid) continue;
                  if (!userTotals[uid]) {
                    userTotals[uid] = {
                      primary: { qty: 0, valueDP: 0, valueTP: 0 },
                      secondary: { qty: 0, valueDP: 0, valueTP: 0 },
                      marketReturn: { qty: 0, valueDP: 0, valueTP: 0 },
                      officeReturn: { qty: 0, valueDP: 0, valueTP: 0 },
                      collection: { amount: 0 }, // Only collection is tracked
                    };
                  }
                  const b = userTotals[uid];
                  const q = t.quantity;
                  const dp = q * t.dp;
                  const tp = q * t.tp;
                  switch (t.type.toLowerCase()) {
                    case "primary":
                      b.primary.qty += q;
                      b.primary.valueDP += dp;
                      b.primary.valueTP += tp;
                      break;
                    case "secondary":
                      b.secondary.qty += q;
                      b.secondary.valueDP += dp;
                      b.secondary.valueTP += tp;
                      break;
                    case "market return":
                      b.marketReturn.qty += q;
                      b.marketReturn.valueDP += dp;
                      b.marketReturn.valueTP += tp;
                      break;
                    case "office return":
                      b.officeReturn.qty += q;
                      b.officeReturn.valueDP += dp;
                      b.officeReturn.valueTP += tp;
                      break;
                  }
                }

                // Process collections only
                collections.forEach((c) => {
                  const uid = c.userId?.toString();
                  if (!uid) return;
                  if (!userTotals[uid]) {
                    userTotals[uid] = {
                      primary: {},
                      secondary: {},
                      marketReturn: {},
                      officeReturn: {},
                      collection: { amount: 0 },
                    };
                  }
                  userTotals[uid].collection.amount += c.amount || 0;
                });

                const openingDP = opening.reduce(
                  (s, o) => s + o.openingValueDP,
                  0,
                );
                const openingTP = opening.reduce(
                  (s, o) => s + o.openingValueTP,
                  0,
                );

                const totals = Object.values(userTotals).reduce(
                  (a, u) => ({
                    pDP: a.pDP + (u.primary?.valueDP || 0),
                    pTP: a.pTP + (u.primary?.valueTP || 0),
                    sDP: a.sDP + (u.secondary?.valueDP || 0),
                    sTP: a.sTP + (u.secondary?.valueTP || 0),
                    mDP: a.mDP + (u.marketReturn?.valueDP || 0),
                    mTP: a.mTP + (u.marketReturn?.valueTP || 0),
                    oDP: a.oDP + (u.officeReturn?.valueDP || 0),
                    oTP: a.oTP + (u.officeReturn?.valueTP || 0),
                    collection: a.collection + (u.collection?.amount || 0),
                  }),
                  {
                    pDP: 0,
                    pTP: 0,
                    sDP: 0,
                    sTP: 0,
                    mDP: 0,
                    mTP: 0,
                    oDP: 0,
                    oTP: 0,
                    collection: 0,
                  },
                );

                const closingDP =
                  openingDP + totals.pDP + totals.mDP - totals.sDP - totals.oDP;
                const closingTP =
                  openingTP + totals.pTP + totals.mTP - totals.sTP - totals.oTP;

                return {
                  outlet: outlet.outlet_name,
                  openingValueDP: openingDP,
                  openingValueTP: openingTP,
                  closingValueDP: closingDP,
                  closingValueTP: closingTP,
                  totalCollection: totals.collection,
                  users: Object.entries(userTotals).map(([uid, t]) => ({
                    userId: uid,
                    name: userMap[uid]?.name || "Unknown",
                    role: userMap[uid]?.role || "SO",
                    asm: userMap[uid]?.asm || "",
                    rsm: userMap[uid]?.rsm || "",
                    som: userMap[uid]?.som || "",
                    zone: userMap[uid]?.zone || "",
                    primary: t.primary,
                    secondary: t.secondary,
                    marketReturn: t.marketReturn,
                    officeReturn: t.officeReturn,
                    collection: t.collection,
                  })),
                };
              } catch (e) {
                console.error(
                  `[Outlet Error] ${outlet.outlet_name}:`,
                  e.message,
                );
                return null;
              }
            }),
          );

          results.push(...batchRes.filter(Boolean));
        }

        res.json({
          success: true,
          data: results,
          period: { start: rangeStart, end: rangeEnd },
          stats: {
            totalOutletsProcessed: results.length,
            totalUsers: Object.keys(userMap).length,
          },
        });
      } catch (err) {
        console.error("[API Error]", err);
        res.status(500).json({
          success: false,
          message: "Internal server error",
          error: err.message,
        });
      }
    });

    // Get financial opening balance
    app.get("/api/opening-due", async (req, res) => {
      try {
        const { outlet, beforeDate } = req.query;

        if (!outlet || !beforeDate) {
          return res.status(400).json({
            success: false,
            message: "Outlet and beforeDate parameters are required",
          });
        }

        // 1. Get fixed opening due from outlet collection
        const outletData = await outlet_collection.findOne({
          outlet_name: outlet,
        });
        const fixedOpeningDue = outletData?.opening_due || 0;

        // 2. Get all transactions before the specified date
        const transactions = await moneyTransactions
          .find({
            outlet,
            date: { $lt: beforeDate },
          })
          .toArray();

        // 3. Calculate net changes
        let netChange = 0;
        transactions.forEach((t) => {
          if (t.type === "primary") {
            netChange += t.amount;
          } else if (t.type === "payment" || t.type === "office return") {
            netChange -= t.amount;
          }
        });

        res.json({
          success: true,
          data: {
            openingDue: fixedOpeningDue + netChange,
          },
        });
      } catch (error) {
        console.error("Opening due error:", error);
        res.status(500).json({
          success: false,
          message: "Error calculating opening due",
          error: error.message,
        });
      }
    });

    // Get financial movement report
    app.get("/api/financial-movement", async (req, res) => {
      try {
        const { outlet, startDate, endDate, asm, rsm, zone } = req.query;

        if (!startDate || !endDate) {
          return res.status(400).json({
            success: false,
            message: "startDate and endDate are required",
          });
        }

        const filters = {
          date: { $gte: startDate, $lte: endDate },
        };

        if (outlet) filters.outlet = outlet;
        if (asm) filters.asm = asm;
        if (rsm) filters.rsm = rsm;
        if (zone) filters.zone = zone;

        // Get opening due
        const openingResponse = await axios.get(
          `http://localhost:5000//api/opening-due`,
          { params: { outlet, beforeDate: startDate } },
        );

        const openingDue = openingResponse.data?.data?.openingDue || 0;

        // Get transactions for the period
        const transactions = await moneyTransactions.find(filters).toArray();

        // Calculate movement during period
        const movement = {
          primary: 0,
          payment: 0,
          officeReturn: 0,
        };

        transactions.forEach((t) => {
          if (t.type === "primary") {
            movement.primary += t.amount;
          } else if (t.type === "payment") {
            movement.payment += t.amount;
          } else if (t.type === "office return") {
            movement.officeReturn += t.amount;
          }
        });

        // Calculate closing due
        const closingDue =
          openingDue +
          movement.primary -
          movement.payment -
          movement.officeReturn;

        res.json({
          success: true,
          data: {
            openingDue,
            primary: movement.primary,
            payment: movement.payment,
            officeReturn: movement.officeReturn,
            closingDue,
            transactions,
          },
        });
      } catch (error) {
        console.error("Financial report error:", error);
        res.status(500).json({
          success: false,
          message: "Error generating financial report",
        });
      }
    });

    // Get area financial movement
    app.get("/api/area-financial-movement", async (req, res) => {
      try {
        const { areaType, areaValue, startDate, endDate } = req.query;

        if (!areaType || !areaValue || !startDate || !endDate) {
          return res.status(400).json({
            success: false,
            message: "All parameters are required",
          });
        }

        const fieldMap = {
          ASM: "asm",
          RSM: "rsm",
          Zone: "zone",
        };

        const fieldName = fieldMap[areaType];
        if (!fieldName) {
          return res.status(400).json({
            success: false,
            message: "Invalid areaType parameter",
          });
        }

        // Get all outlets for this area
        const outlets = await moneyTransactions.distinct("outlet", {
          [fieldName]: areaValue,
        });

        if (outlets.length === 0) {
          return res.json({ success: true, data: [] });
        }

        // Get opening due for all outlets
        const openingPromises = outlets.map((outlet) =>
          axios.get("http://localhost:5000//api/opening-due", {
            params: { outlet, beforeDate: startDate },
          }),
        );

        const openingResponses = await Promise.all(openingPromises);
        const totalOpeningDue = openingResponses
          .filter((res) => res.data?.success)
          .reduce((sum, res) => sum + (res.data.data?.openingDue || 0), 0);

        // Get transactions for the period
        const transactions = await moneyTransactions
          .find({
            [fieldName]: areaValue,
            date: { $gte: startDate, $lte: endDate },
          })
          .toArray();

        // Calculate movement during period
        const movement = {
          primary: 0,
          payment: 0,
          officeReturn: 0,
        };

        transactions.forEach((t) => {
          if (t.type === "primary") {
            movement.primary += t.amount;
          } else if (t.type === "payment") {
            movement.payment += t.amount;
          } else if (t.type === "office return") {
            movement.officeReturn += t.amount;
          }
        });

        // Calculate closing due
        const closingDue =
          totalOpeningDue +
          movement.primary -
          movement.payment -
          movement.officeReturn;

        res.json({
          success: true,
          data: {
            openingDue: totalOpeningDue,
            primary: movement.primary,
            payment: movement.payment,
            officeReturn: movement.officeReturn,
            closingDue,
            transactions,
            outletCount: outlets.length,
          },
        });
      } catch (error) {
        console.error("Area financial report error:", error);
        res.status(500).json({
          success: false,
          message: "Error generating area financial report",
        });
      }
    });

    // Create (POST) a new TDDA record
    app.post("/tdda", async (req, res) => {
      try {
        const tddaData = req.body;

        if (!tddaData.userId || !tddaData.month) {
          return res.status(400).json({
            error: "User ID and month are required",
          });
        }

        const result = await tdda.insertOne(tddaData);
        res.status(201).json({
          message: "TDDA record created successfully",
          id: result.insertedId,
        });
      } catch (err) {
        console.error("Error creating TDDA record:", err);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    // Read (GET) TDDA records with filtering
    app.get("/tdda", async (req, res) => {
      try {
        const { userId, month } = req.query;
        const query = {};

        if (userId) query.userId = userId;
        if (month) query.month = month;

        const tddas = await tdda.find(query).toArray();
        res.json(tddas);
      } catch (err) {
        console.error("Error fetching TDDA records:", err);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    // Update (PUT) a TDDA record
    app.put("/tdda/:id", async (req, res) => {
      try {
        const updateData = req.body;
        const id = req.params.id;

        // Remove _id from update data if present
        delete updateData._id;

        const result = await tdda.updateOne(
          { _id: new ObjectId(id) },
          { $set: updateData },
        );

        if (result.matchedCount === 0) {
          return res.status(404).json({ error: "TDDA record not found" });
        }

        res.json({ message: "TDDA record updated successfully" });
      } catch (err) {
        console.error("Error updating TDDA record:", err);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    // Delete a TDDA record
    app.delete("/tdda/:id", async (req, res) => {
      try {
        const result = await tdda.deleteOne({
          _id: new ObjectId(req.params.id),
        });

        if (result.deletedCount === 0) {
          return res.status(404).json({ error: "TDDA record not found" });
        }

        res.json({ message: "TDDA record deleted successfully" });
      } catch (err) {
        console.error("Error deleting TDDA record:", err);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    // Get all users with TDDA records
    app.get("/tdda/users", async (req, res) => {
      try {
        const tddausers = await tdda.distinct("userId", {});

        // Convert each userId to ObjectId (if valid)
        const userIds = tddausers
          .map((id) => {
            try {
              return id; // Convert to ObjectId
            } catch (err) {
              console.error(`Invalid ObjectId: ${id}`);
              return null; // Skip invalid IDs
            }
          })
          .filter((id) => id !== null); // Remove null entries

        const userDetails = await users
          .find({ _id: { $in: userIds } })
          .toArray();
        res.json(userDetails);
      } catch (err) {
        console.error("Error fetching TDDA users:", err);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    // Get TDDA data aggregated by user and month
    app.get("/tdda/admin-report", async (req, res) => {
      try {
        const { userId, month } = req.query;

        if (!userId || !month) {
          return res
            .status(400)
            .json({ error: "User ID and month are required" });
        }

        // Get records
        const records = await tdda
          .find({
            userId: userId,
            month: month,
          })
          .toArray();

        if (!records || records.length === 0) {
          return res.status(404).json({ error: "No records found" });
        }

        // Transform records into daily expenses format
        const dailyExpenses = records.map((record) => ({
          ...record.dailyExpense,
          date: record.date,
          userName: record.name,
          designation: record.designation,
          area: record.area,
        }));

        // Calculate summary
        const summary = {
          totalWorkingDays: dailyExpenses.filter((e) => e.from || e.to).length,
          totalExpense: dailyExpenses.reduce(
            (sum, day) => sum + (parseFloat(day.totalExpense) || 0, 0),
          ),
        };

        res.json({
          userInfo: {
            name: records[0]?.name,
            designation: records[0]?.designation,
            area: records[0]?.area,
            month: records[0]?.month,
          },
          dailyExpenses,
          summary,
        });
      } catch (err) {
        console.error("Error generating admin report:", err);
        res.status(500).json({
          error: "Internal server error",
          details:
            process.env.NODE_ENV === "development" ? err.message : undefined,
        });
      }
    });

    // Add this to your backend API routes
    app.get("/api/tdda-summary", async (req, res) => {
      try {
        const { month } = req.query;
        if (!month) {
          return res.status(400).json({ error: "Month parameter is required" });
        }

        // Get all users with TDDA records
        const usersWithTDDA = await tdda.distinct("userId", { month });

        // Get TDDA data for each user
        const tddaData = await Promise.all(
          usersWithTDDA.map(async (userId) => {
            const records = await tdda.find({ userId, month }).toArray();
            const totalExpense = records.reduce((sum, record) => {
              return sum + (parseFloat(record.dailyExpense.totalExpense) || 0);
            }, 0);

            return {
              userId,
              totalExpense,
            };
          }),
        );

        // Convert to a map for easy lookup
        const tddaMap = tddaData.reduce((acc, item) => {
          acc[item.userId] = item.totalExpense;
          return acc;
        }, {});

        res.json({ success: true, data: tddaMap });
      } catch (err) {
        console.error("Error fetching TDDA summary:", err);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    // Submit a new order request
    app.post("/primary-request", async (req, res) => {
      try {
        // Check if invoice already exists
        const existing = await orderRequests.findOne({
          invoiceNo: req.body.invoiceNo,
        });
        if (existing) {
          return res.status(400).json({
            success: false,
            message: "Invoice number already exists. Please use a unique one.",
          });
        }

        const request = {
          ...req.body,
          createdAt: new Date(),
          status: "pending",
        };

        const result = await orderRequests.insertOne(request);
        res.status(201).json({ success: true, requestId: result.insertedId });
      } catch (err) {
        console.error("Primary request error:", err);
        res
          .status(500)
          .json({ success: false, message: "Internal Server Error" });
      }
    });

    // Get all requests with optional type filter
    app.get("/order-requests", async (req, res) => {
      try {
        const { status, type, startDate, endDate, invoiceNo, outlet } =
          req.query;
        const user = JSON.parse(req.headers["wms-user"] || "{}");
        const query = {};

        // Outlet filter: Prioritize query.outlet over user.outlet
        if (outlet && outlet !== "all") {
          query.outlet = outlet;
        } else if (user.outlet) {
          query.outlet = user.outlet;
        }

        // Status filter
        if (status && status !== "all") {
          query.status = status;
        }

        // Type filter
        if (type && type !== "all") {
          query.type = type;
        }

        // Date range filter: Default to current month if no dates provided
        query.date = {};
        if (startDate) {
          query.date.$gte = dayjs(startDate)
            .startOf("day")
            .format("YYYY-MM-DD");
        } else {
          query.date.$gte = dayjs().startOf("month").format("YYYY-MM-DD");
        }
        if (endDate) {
          query.date.$lte = dayjs(endDate)
            .endOf("day")
            .format("YYYY-MM-DD 23:59:59");
        } else {
          query.date.$lte = dayjs()
            .endOf("month")
            .format("YYYY-MM-DD 23:59:59");
        }

        // Remove date filter if both startDate and endDate are explicitly empty
        if (
          !startDate &&
          !endDate &&
          req.query.startDate === "" &&
          req.query.endDate === ""
        ) {
          delete query.date;
        }

        // Invoice number filter
        if (invoiceNo) {
          query.$or = [
            { invoiceNo: { $regex: invoiceNo, $options: "i" } },
            { transferNo: { $regex: invoiceNo, $options: "i" } },
          ];
        }

        const requests = await orderRequests
          .find(query)
          .sort({ date: -1 })
          .toArray();

        // Enrich items with product names
        const enrichedRequests = await Promise.all(
          requests.map(async (request) => {
            let itemsWithNames;
            if (request.type === "transfer") {
              itemsWithNames = await Promise.all(
                request.items.map(async (item) => {
                  const fromProduct = await products.findOne(
                    { barcode: item.from_barcode },
                    { projection: { name: 1 } },
                  );
                  const toProduct = await products.findOne(
                    { barcode: item.to_barcode },
                    { projection: { name: 1 } },
                  );
                  return {
                    ...item,
                    from_name: fromProduct?.name || "Unknown Product",
                    to_name: toProduct?.name || "Unknown Product",
                  };
                }),
              );
            } else {
              itemsWithNames = await Promise.all(
                request.items.map(async (item) => {
                  const product = await products.findOne(
                    { barcode: item.barcode },
                    { projection: { name: 1 } },
                  );
                  return { ...item, name: product?.name || "Unknown Product" };
                }),
              );
            }
            return { ...request, items: itemsWithNames };
          }),
        );

        res.json({
          success: true,
          data: enrichedRequests,
        });
      } catch (err) {
        console.error("[order-requests]", err);
        res.status(500).json({
          success: false,
          message: "Internal server error",
        });
      }
    });
    // Update request status and process stock updates on confirmation
    app.put("/order-requests/:id", async (req, res) => {
      try {
        const { id } = req.params;
        const { status } = req.body;

        // Fetch the request to check its type and data
        const request = await orderRequests.findOne({ _id: new ObjectId(id) });
        if (!request) {
          return res
            .status(404)
            .json({ success: false, message: "Request not found" });
        }

        // Update the request status
        const result = await orderRequests.updateOne(
          { _id: new ObjectId(id) },
          { $set: { status } },
        );

        if (result.modifiedCount === 0) {
          return res
            .status(400)
            .json({ success: false, message: "Failed to update status" });
        }

        // If the request is confirmed, process stock updates and transactions
        if (status === "confirmed") {
          try {
            const promises = request.items.map(async (item) => {
              // Common fields for stock transaction
              const transaction = {
                barcode: item.barcode,
                outlet: request.outlet,
                type: request.type,
                invoiceNo: request.invoiceNo,
                partyName: request.partyName || "",
                asm: request.asm || "",
                rsm: request.rsm || "",
                zone: request.zone,
                quantity: item.quantity || item.qty || item.newStock || 0,
                date: request.date,
                user: request.user || request.name,
                userID: request.userID || request.userId,
                dp: item.dp || item.editableDP || 0,
                tp: item.tp || item.editableTP || 0,
                transfered_to: "",
                transfered_from: "",
              };

              // Handle stock updates based on request type
              if (request.type === "opening" && item.canEdit) {
                // Update outlet stock for opening requests
                const updateFields = {
                  [`outlet_stocks.${request.outlet}.currentStock`]:
                    item.newStock,
                  [`outlet_stocks.${request.outlet}.currentStockValueDP`]:
                    item.newStock * item.editableDP,
                  [`outlet_stocks.${request.outlet}.currentStockValueTP`]:
                    item.newStock * item.editableTP,
                  [`outlet_stocks.${request.outlet}.openingStockValueDP`]:
                    item.newStock * item.editableDP,
                  [`outlet_stocks.${request.outlet}.openingStockValueTP`]:
                    item.newStock * item.editableTP,
                  [`outlet_stocks.${request.outlet}.openingStock`]:
                    item.newStock,
                };

                await outletStockCollection.updateOne(
                  { barcode: item.barcode },
                  { $set: updateFields },
                );
              } else if (
                request.type === "primary" ||
                request.type === "return"
              ) {
                // Update outlet stock for primary requests (increment stock)
                const qty = item.quantity || item.qty || item.newStock || 0;
                await outletStockCollection.updateOne(
                  { barcode: item.barcode },
                  {
                    $inc: {
                      [`outlet_stocks.${request.outlet}.currentStock`]: qty,
                      [`outlet_stocks.${request.outlet}.currentStockValueDP`]:
                        qty * (item.dp || item.editableDP || 0),
                      [`outlet_stocks.${request.outlet}.currentStockValueTP`]:
                        qty * (item.tp || item.editableTP || 0),
                    },
                  },
                  { upsert: true },
                );
              } else if (request.type === "secondary") {
                // Update outlet stock for secondary requests (decrement stock)
                const qty = item.quantity || item.qty || item.newStock || 0;
                await outletStockCollection.updateOne(
                  { barcode: item.barcode },
                  {
                    $inc: {
                      [`outlet_stocks.${request.outlet}.currentStock`]: -qty,
                      [`outlet_stocks.${request.outlet}.currentStockValueDP`]:
                        -qty * (item.dp || item.editableDP || 0),
                      [`outlet_stocks.${request.outlet}.currentStockValueTP`]:
                        -qty * (item.tp || item.editableTP || 0),
                    },
                  },
                );
              } else if (request.type === "office return") {
                const qty = item.quantity || item.qty || item.newStock || 0;
                const stockDoc = await outletStockCollection.findOne({
                  barcode: item.barcode,
                });
                const currentStock =
                  stockDoc?.outlet_stocks?.[request.outlet]?.currentStock || 0;
                if (currentStock < qty) {
                  throw new Error(
                    `Insufficient stock for ${item.barcode}. Available: ${currentStock}, Requested: ${qty}`,
                  );
                }
                // Deduct from source (request.outlet)
                await outletStockCollection.updateOne(
                  { barcode: item.barcode },
                  {
                    $inc: {
                      [`outlet_stocks.${request.outlet}.currentStock`]: -qty,
                      [`outlet_stocks.${request.outlet}.currentStockValueDP`]:
                        -qty * (item.dp || item.editableDP || 0),
                      [`outlet_stocks.${request.outlet}.currentStockValueTP`]:
                        -qty * (item.tp || item.editableTP || 0),
                    },
                  },
                  { upsert: true },
                );
                // Add to destination (request.transferToOutlet)
                await outletStockCollection.updateOne(
                  { barcode: item.barcode },
                  {
                    $inc: {
                      [`outlet_stocks.${request.transferToOutlet}.currentStock`]:
                        qty,
                      [`outlet_stocks.${request.transferToOutlet}.currentStockValueDP`]:
                        qty * (item.dp || item.editableDP || 0),
                      [`outlet_stocks.${request.transferToOutlet}.currentStockValueTP`]:
                        qty * (item.tp || item.editableTP || 0),
                    },
                  },
                  { upsert: true },
                );
                // Record transaction for source
                await stock_transactions.insertOne({
                  barcode: item.barcode,
                  outlet: request.outlet,
                  invoiceNo: request.invoiceNo,
                  type: "office return",
                  asm: request.asm || "",
                  rsm: request.rsm || "",
                  zone: request.zone,
                  quantity: qty,
                  date: request.date,
                  user: request.user || request.name,
                  userID: request.userID || request.userId,
                  dp: item.dp || item.editableDP || 0,
                  tp: item.tp || item.editableTP || 0,
                  transfered_to: request.transferToOutlet,
                  transfered_from: "",
                });
                // Record transaction for destination
                await stock_transactions.insertOne({
                  barcode: item.barcode,
                  outlet: request.transferToOutlet,
                  invoiceNo: request.invoiceNo,
                  type: "market return",
                  asm: request.asm || "",
                  rsm: request.rsm || "",
                  zone: request.zone,
                  quantity: qty,
                  date: request.date,
                  user: request.user || request.name,
                  userID: request.userID || request.userId,
                  dp: item.dp || item.editableDP || 0,
                  tp: item.tp || item.editableTP || 0,
                  transfered_to: "",
                  transfered_from: request.outlet,
                });
                return; // Skip the default transaction insert below
              } else if (request.type === "transfer") {
                const qty = item.quantity || item.qty || item.newStock || 0;
                const fromStockDoc = await outletStockCollection.findOne({
                  barcode: item.from_barcode,
                });
                const outletStocks = fromStockDoc?.outlet_stocks?.[
                  request.outlet
                ] || {
                  currentStock: 0,
                  currentStockValueDP: 0,
                  currentStockValueTP: 0,
                };
                const currentFromStock = outletStocks.currentStock;
                if (currentFromStock < qty) {
                  throw new Error(
                    `Insufficient stock for ${item.from_barcode}. Available: ${currentFromStock}, Requested: ${qty}`,
                  );
                }
                const unitDP =
                  currentFromStock > 0
                    ? outletStocks.currentStockValueDP / currentFromStock
                    : 0;
                const unitTP =
                  currentFromStock > 0
                    ? outletStocks.currentStockValueTP / currentFromStock
                    : 0;
                const valueDP = qty * unitDP;
                const valueTP = qty * unitTP;

                // Deduct from fresh (from_barcode)
                await outletStockCollection.updateOne(
                  { barcode: item.from_barcode },
                  {
                    $inc: {
                      [`outlet_stocks.${request.outlet}.currentStock`]: -qty,
                      [`outlet_stocks.${request.outlet}.currentStockValueDP`]:
                        -valueDP,
                      [`outlet_stocks.${request.outlet}.currentStockValueTP`]:
                        -valueTP,
                    },
                  },
                );

                // Add to damaged (to_barcode)
                await outletStockCollection.updateOne(
                  { barcode: item.to_barcode },
                  {
                    $inc: {
                      [`outlet_stocks.${request.outlet}.currentStock`]: qty,
                      [`outlet_stocks.${request.outlet}.currentStockValueDP`]:
                        valueDP,
                      [`outlet_stocks.${request.outlet}.currentStockValueTP`]:
                        valueTP,
                    },
                  },
                  { upsert: true },
                );

                // Record transaction for fresh (out)
                await stock_transactions.insertOne({
                  barcode: item.from_barcode,
                  outlet: request.outlet,
                  type: "secondary",
                  invoiceNo: request.transferNo || "",
                  asm: request.asm || "",
                  rsm: request.rsm || "",
                  zone: request.zone,
                  quantity: qty,
                  date: request.date,
                  user: request.user || request.name,
                  userID: request.userID || request.userId,
                  dp: unitDP,
                  tp: unitTP,
                  transfered_to: "",
                  transfered_from: "",
                });

                // Record transaction for damaged (in)
                await stock_transactions.insertOne({
                  barcode: item.to_barcode,
                  outlet: request.outlet,
                  type: "primary",
                  invoiceNo: request.transferNo || "",
                  asm: request.asm || "",
                  rsm: request.rsm || "",
                  zone: request.zone,
                  quantity: qty,
                  date: request.date,
                  user: request.user || request.name,
                  userID: request.userID || request.userId,
                  dp: unitDP,
                  tp: unitTP,
                  transfered_to: "",
                  transfered_from: "",
                });
                return; // Skip the default transaction insert below
              }

              // Record stock transaction (for other types)
              await stock_transactions.insertOne(transaction);
            });

            await Promise.all(promises);
          } catch (err) {
            console.error("Stock update error on confirmation:", err);
            return res.status(500).json({
              success: false,
              message: "Failed to process stock updates",
            });
          }
        }

        res.json({ success: true, modified: result.modifiedCount });
      } catch (err) {
        console.error("Update request error:", err);
        res
          .status(500)
          .json({ success: false, message: "Internal server error" });
      }
    });

    // Update request items without changing status or affecting stock
    app.put("/order-requests/:id/items", async (req, res) => {
      try {
        const { id } = req.params;
        const { items, date } = req.body;

        // Validate request body
        if (!items || !Array.isArray(items) || items.length === 0) {
          return res.status(400).json({
            success: false,
            message: "Items array is required and cannot be empty",
          });
        }

        // Validate date if provided
        // if (date && !dayjs(date).isValid()) {
        //   return res
        //     .status(400)
        //     .json({ success: false, message: "Invalid date format" });
        // }

        // Fetch the request to ensure it exists
        const request = await orderRequests.findOne({ _id: new ObjectId(id) });
        if (!request) {
          return res
            .status(404)
            .json({ success: false, message: "Request not found" });
        }

        // Prevent editing confirmed requests
        if (request.status === "confirmed") {
          return res.status(400).json({
            success: false,
            message: "Can only edit pending requests",
          });
        }

        // Prepare update fields
        const updateFields = {
          items,
          status: "pending",
          date,
        };
        // if (date) {
        //   updateFields.date = date; // Convert to Date object for MongoDB
        // }

        // Update the request with new items, status, and date (if provided)
        const result = await orderRequests.updateOne(
          { _id: new ObjectId(id) },
          { $set: updateFields },
        );

        if (result.modifiedCount === 0) {
          return res
            .status(400)
            .json({ success: false, message: "Failed to update request" });
        }

        res.json({ success: true });
      } catch (err) {
        console.error("Update items error:", err);
        res
          .status(500)
          .json({ success: false, message: "Internal server error" });
      }
    });
    // Delete request
    app.delete("/order-requests/:id", async (req, res) => {
      try {
        const { id } = req.params;

        const request = await orderRequests.findOne({ _id: new ObjectId(id) });
        if (!request) {
          return res
            .status(404)
            .json({ success: false, message: "Request not found" });
        }

        const result = await orderRequests.deleteOne({ _id: new ObjectId(id) });

        if (result.deletedCount === 0) {
          return res
            .status(404)
            .json({ success: false, message: "Request not found" });
        }

        res.json({ success: true });
      } catch (err) {
        console.error("Delete request error:", err);
        res
          .status(500)
          .json({ success: false, message: "Internal server error" });
      }
    });
    app.delete("/order-requests/invoice/:invoiceNo", async (req, res) => {
      try {
        const { invoiceNo } = req.params;
        const decodedInvoiceNo = decodeURIComponent(invoiceNo); // Decode to handle special characters like '/'

        const result = await orderRequests.deleteOne({
          invoiceNo: decodedInvoiceNo,
        });

        if (result.deletedCount === 0) {
          return res.status(404).json({
            success: false,
            message: `No order request found for invoiceNo: ${decodedInvoiceNo}`,
          });
        }

        res.json({
          success: true,
          message: `Order request for invoiceNo: ${decodedInvoiceNo} deleted successfully`,
        });
      } catch (err) {
        console.error("Delete order request by invoiceNo error:", err);
        res.status(500).json({
          success: false,
          message: "Internal server error",
        });
      }
    });

    app.put("/update-pricelist", async (req, res) => {
      try {
        // Step 1: Fetch all products from the "products" collection
        const allProducts = await products.find({}).toArray();

        // Step 2: Prepare bulk update operations
        const bulkUpdates = allProducts
          .filter(
            (product) =>
              product.tp !== undefined &&
              product.dp !== undefined &&
              product.mrp !== undefined,
          )
          .map((product) => {
            // Parse numeric values from tp/dp/mrp (in case they are strings)
            const tp = parseFloat(product.tp);
            const dp = parseFloat(product.dp);
            const mrp = parseFloat(product.mrp);

            // Skip if any value failed to parse
            if (isNaN(tp) || isNaN(dp) || isNaN(mrp)) {
              return null;
            }

            // Step 3: Build discounted price list
            const priceList = {
              mt: {
                tp: +(tp * 0.95).toFixed(2), // 5% less
                dp: +(dp * 0.95).toFixed(2),
                mrp: +(mrp * 0.95).toFixed(2),
              },
              shwapno: {
                tp: +(tp * 0.92).toFixed(2), // 8% less
                dp: +(dp * 0.92).toFixed(2),
                mrp: +(mrp * 0.92).toFixed(2),
              },
              agora: {
                tp: +(tp * 0.9).toFixed(2), // 10% less
                dp: +(dp * 0.9).toFixed(2),
                mrp: +(mrp * 0.9).toFixed(2),
              },
            };

            return {
              updateOne: {
                filter: { _id: product._id },
                update: { $set: { priceList } },
              },
            };
          })
          .filter(Boolean); // remove nulls

        // Step 4: Run bulk update
        if (bulkUpdates.length === 0) {
          return res
            .status(400)
            .json({ error: "No valid products to update." });
        }

        const result = await products.bulkWrite(bulkUpdates);

        // Step 5: Return success response
        res.status(200).json({
          message: "Products updated with priceList successfully.",
          matchedCount: result.matchedCount,
          modifiedCount: result.modifiedCount,
        });
      } catch (error) {
        console.error("Error updating priceList:", error);
        res.status(500).json({ error: "Internal server error" });
      }
    });
    // Add this to your server routes
    app.put("/update-products-with-new-pricelevel", async (req, res) => {
      try {
        const { priceLevelName } = req.body;

        // Fetch all products
        const allProducts = await products.find({}).toArray();

        // Prepare bulk update operations
        const bulkUpdates = allProducts
          .filter(
            (product) =>
              product.tp !== undefined &&
              product.dp !== undefined &&
              product.mrp !== undefined,
          )
          .map((product) => {
            // Parse numeric values from tp/dp/mrp
            const tp = parseFloat(product.tp);
            const dp = parseFloat(product.dp);
            const mrp = parseFloat(product.mrp);

            // Skip if any value failed to parse
            if (isNaN(tp) || isNaN(dp) || isNaN(mrp)) {
              return null;
            }

            // Create the new price level entry with default values
            const newPriceLevel = {
              tp: tp, // Default to base TP price
              dp: dp, // Default to base DP price
              mrp: mrp, // Default to base MRP price
            };

            return {
              updateOne: {
                filter: { _id: product._id },
                update: {
                  $set: {
                    [`priceList.${priceLevelName}`]: newPriceLevel,
                  },
                },
              },
            };
          })
          .filter(Boolean); // remove nulls

        if (bulkUpdates.length === 0) {
          return res
            .status(400)
            .json({ error: "No valid products to update." });
        }

        const result = await products.bulkWrite(bulkUpdates);

        res.status(200).json({
          message: `Added ${priceLevelName} price level to all products`,
          matchedCount: result.matchedCount,
          modifiedCount: result.modifiedCount,
        });
      } catch (error) {
        console.error("Error updating products with new price level:", error);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    app.put("/update-pricelevel-in-products", async (req, res) => {
      try {
        const { oldName, newName, newDisplayName } = req.body;

        // Update all products that have this price level
        const result = await products.updateMany(
          { [`priceList.${oldName}`]: { $exists: true } },
          {
            $rename: { [`priceList.${oldName}`]: [`priceList.${newName}`] },
            $set: { [`priceList.${newName}.displayName`]: newDisplayName },
          },
        );

        res.status(200).json({
          message: `Updated price level from ${oldName} to ${newName} in all products`,
          matchedCount: result.matchedCount,
          modifiedCount: result.modifiedCount,
        });
      } catch (error) {
        console.error("Error updating price level in products:", error);
        res.status(500).json({ error: "Internal server error" });
      }
    });
    app.put("/remove-pricelevel-from-products", async (req, res) => {
      try {
        const { priceLevelName } = req.body;

        // Remove this price level from all products
        const result = await products.updateMany(
          { [`priceList.${priceLevelName}`]: { $exists: true } },
          { $unset: { [`priceList.${priceLevelName}`]: "" } },
        );

        res.status(200).json({
          message: `Removed ${priceLevelName} from all products`,
          matchedCount: result.matchedCount,
          modifiedCount: result.modifiedCount,
        });
      } catch (error) {
        console.error("Error removing price level from products:", error);
        res.status(500).json({ error: "Internal server error" });
      }
    });

    app.get("/api/pricelevels", async (req, res) => {
      try {
        const priceLevels = await pricelevel.find({}).toArray();
        res.json(priceLevels);
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });
    app.post("/api/pricelevels", async (req, res) => {
      try {
        const { name, displayName } = req.body;
        console.log(name, displayName);
        const result = await pricelevel.insertOne({
          name,
          displayName,
          createdAt: new Date(),
          updatedAt: new Date(),
        });
        res.status(201).json(result);
      } catch (err) {
        res.status(400).json({ error: err.message });
      }
    });
    app.put("/api/pricelevels/:name", async (req, res) => {
      try {
        const { name } = req.params;
        const { displayName } = req.body;
        const result = await pricelevel.updateOne(
          { name },
          { $set: { displayName, updatedAt: new Date() } },
        );
        if (result.matchedCount === 0) {
          return res.status(404).json({ error: "Price level not found" });
        }
        res.json({ message: "Price level updated successfully" });
      } catch (err) {
        res.status(400).json({ error: err.message });
      }
    });
    app.delete("/api/pricelevels/:name", async (req, res) => {
      try {
        const { name } = req.params;
        const result = await pricelevel.deleteOne({ name });
        if (result.deletedCount === 0) {
          return res.status(404).json({ error: "Price level not found" });
        }
        res.json({ message: "Price level deleted successfully" });
      } catch (err) {
        res.status(400).json({ error: err.message });
      }
    });
    app.get("/api/pricelevels/:name", async (req, res) => {
      try {
        const { name } = req.params;
        const priceLevel = await pricelevel.findOne({ name });
        if (!priceLevel) {
          return res.status(404).json({ error: "Price level not found" });
        }
        res.json(priceLevel);
      } catch (err) {
        res.status(500).json({ error: err.message });
      }
    });

    app.post("/bulk-import-products", async (req, res) => {
      try {
        const { updatedProducts } = req.body;

        if (!updatedProducts || !Array.isArray(updatedProducts)) {
          return res.status(400).json({
            success: false,
            error: "Invalid products data format",
          });
        }

        // Prepare bulk operations
        const bulkOps = updatedProducts.map((product) => {
          // Create a copy of the product without _id for the update
          const { _id, ...updateData } = product;

          return {
            updateOne: {
              filter: { _id: new ObjectId(_id) }, // Convert to ObjectId
              update: { $set: updateData }, // Update all fields except _id
              upsert: true,
            },
          };
        });

        // Execute bulk write
        const result = await products.bulkWrite(bulkOps);

        res.json({
          success: true,
          insertedCount: result.upsertedCount || 0,
          updatedCount: result.modifiedCount || 0,
          matchedCount: result.matchedCount || 0,
          totalProcessed: updatedProducts.length,
        });
      } catch (error) {
        console.error("Bulk import error:", error);
        res.status(500).json({
          success: false,
          error: "Failed to process bulk import",
          details: error.message,
        });
      }
    });
  } finally {
    // Optional: Can handle client connection closing here if necessary.
  }
}

run().catch(console.dir);

app.get("/", (req, res) => {
  res.send("Server is running");
});

app.listen(port, () => {
  console.log("Listening at port", port);
});
