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
const mongoURI = "mongodb://192.168.0.59:27017/wh-pos-test";

const client = new MongoClient(mongoURI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
  serverApi: ServerApiVersion.v1,
});

async function run() {
  try {
    const products = client.db("wh-pos-test").collection("products");
    const users = client.db("wh-pos-test").collection("users");
    const outlet_collection = client
      .db("wh-pos-test")
      .collection("outlet_collection");
    const stock_transactions = client
      .db("wh-pos-test")
      .collection("stock_transactions");
    const outletStockCollection = client
      .db("wh-pos-test")
      .collection("outlet_stock");
    const category = client.db("wh-pos-test").collection("category");
    const brand = client.db("wh-pos-test").collection("brand");
    const orderRequests = client.db("wh-pos-test").collection("orderRequests");


    app.post("/add-outlet-stock", async (req, res) => {
      try {
        const productsCollection = client
          .db("wh-pos-test")
          .collection("products");

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

    app.post("/create-product-with-stocks", async (req, res) => {
      try {
        const { productData } = req.body;
        const productsCollection = client
          .db("wh-pos-test")
          .collection("products");
        const outletStockCollection = client
          .db("wh-pos-test")
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
        const updateFields = {
          [`outlet_stocks.${outlet}.currentStock`]: newStock,
        };

        if (openingStockValueDP !== undefined) {
          updateFields[`outlet_stocks.${outlet}.openingStock`] = newStock;
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
          `http://175.29.181.245:9001/api/opening-stocks`,
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

    app.put("/stock-transactions/invoice/:invoiceNo", async (req, res) => {
      const { invoiceNo } = req.params;
      const { date, type, partyName, items } = req.body; // items: [{ _id, quantity }]

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
          axios.get("http://175.29.181.245:9001/api/opening-stocks", {
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
          "http://175.29.181.245:9001//get-outlets",
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
            "http://175.29.181.245:9001/api/stock-movement",
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
  } finally {
  }
}

run().catch(console.dir);

app.get("/", (req, res) => {
  res.send("Server is running");
});

app.listen(port, () => {
  console.log("Listening at port", port);
});
