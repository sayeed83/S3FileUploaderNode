const fs = require("fs");
const PDFDocument = require('/opt/node_modules/pdfkit');
// const { fontSize } = require("pdfkit/js/mixins/fonts");
// https://github.com/PSPDFKit-labs/pdfkit-invoice/blob/master/createInvoice.js // Don't remove

function createInvoice(invoice, path) {
    let doc = new PDFDocument({ size: "A4", margin: 40 });
    
    // generateHeader(doc);
    gerateFirstPage(doc);
    // generateCustomerInformation(doc, invoice);
    // generateInvoiceTable(doc, invoice);
    generateFooter(doc, 1);
    doc
    .addPage()
    // generateHeader(doc);
    
    gerateSecondPage(doc);
    generateFooter(doc, 2);
    // .fontSize(25)
    // .text('Here is some vector graphics...', 100, 100);

    doc.end();
    console.log(' pdfPromise called ')
    const buffers = []
    doc.on("data", buffers.push.bind(buffers))
    doc.on("end", async () => {
        console.log(" success reading pdf ", buffers);
        // return buffers;
        return Buffer.concat(buffers)
        // resolve(pdfData)
    })
    // doc.pipe(fs.createWriteStream(path));
}

function generateHeader(doc) {
  doc
    .image("./test1.png", 50, 40, { width: 100 })
    .fillColor("#444444")
    .image("./test2.png", 375, 40, { width: 200 })
    .fillColor("#444444")
    .moveDown();
}

function gerateFirstPage(doc) {
    let customerInformationTop = 150;
    doc
    .fontSize(20)
    .font("Helvetica-Bold")
    .text("LEGAL NOTICE", 225, customerInformationTop)
    .fontSize(18)
    .font("Helvetica")
    .text("Ref no:", 50, customerInformationTop+30)
    .text("#2110468392865397", 115, customerInformationTop+30)
    .text("Date:", 50, customerInformationTop+50)
    .text("2nd Nov, 2020", 100, customerInformationTop+50)
    .fontSize(18)
    .font("Helvetica-Bold")
    .text("To,", 50, customerInformationTop+90)
    .fontSize(14)
    .font("Helvetica")
    .text("VENKATESH VEMULA 1-203 peddakandukur yadagirigutta yadadribhongir watartank 508286", 50, customerInformationTop+115)
    .fontSize(13)
    .font("Helvetica")
    .text("Dear Sir,", 50, customerInformationTop+175)

    .fontSize(12)
    .font("Helvetica")
    .text("I, for and on behalf of our client namely, M/S SI-CREVA CAPITAL SERVICES PRIVATE LIMITED, 10/A/2, New Sion Chs, Swami Vallabhdas Marg, Road No. 24, Sindhi Colony, Sion, Mumbai, 400022 (“Client”) give notice to you as under: -", 50, customerInformationTop+190)
    
    .font("Helvetica-Bold")
    .fontSize(12)
    .text("1.", 60, customerInformationTop+250)
    .font("Helvetica")
    .text("That Our “Client” is Financial Technology Solution Company bearing registration number 282573, given by Registrar of Companies, Mumbai, that provides a technology platform to enable use of instant EMI or instalment solution by consumers for purchase of goods such as electronics, furniture, services etc. through its application called Kissht. It is engaged in the business of online merchant acquisition and financing of consumer loans for purchase of goods and services from digital channels such as online and mobile. The Financing is facilitated by Kissht through its Non-Banking Financial Partners and tie ups with other Financial Institutions and banks.", 75, customerInformationTop+250)

    .font("Helvetica-Bold")
    .fontSize(12)
    .text("2.", 60, customerInformationTop+370)
    .font("Helvetica")
    .text("That you {{loaneeName}} residing at {{loaneeAddress}} are a customer of my client.", 75, customerInformationTop+370)
    
    .font("Helvetica-Bold")
    .fontSize(12)
    .text("3.", 60, customerInformationTop+400)
    .font("Helvetica")
    .text("ThatyouVENKA TESHVEMULArequestedtoourclientforprovidingLoanRs.{{amount}} {vide agreement dated 4th Nov 2019 (Memorandum of Understanding)} requested for loan (Loan ID {{loanId}}) and represented, assured and acknowledged via email of the mandate and payment Amount for providing such Financial Assistance and accordingly loan was given to you from my client.", 75, customerInformationTop+400)
    
    .font("Helvetica-Bold")
    .fontSize(12)
    .text("4.", 60, customerInformationTop+480)
    .font("Helvetica")
    .text("Our client understands that the COVID times are difficult for individual & hence our client Kissht offered moratorium to its customers during COVID times as per RBI guidelines.", 75, customerInformationTop+480)

    .font("Helvetica-Bold")
    .fontSize(12)
    .text("5.", 60, customerInformationTop+525)
    .font("Helvetica")
    .text("That on 1/27/2020 Your last payment was received & RBI announced moratorium on 1st Mar 2020 and sufficient time was allotted to you for payment of outstanding loan EMI amount but still we have not received any payment from your end. Kindly note that your outstanding loan amount pending as on date is 9696.5. We have tried to reach out multiple times through calls, SMS and in person visit but in vain.", 75, customerInformationTop+525)
    
    .moveDown();
}

function gerateSecondPage(doc) {
    let customerInformationTop = 155;
    doc
    .font("Helvetica-Bold")
    .fontSize(12)
    .text("6.", 60, customerInformationTop)
    .font("Helvetica")
    .text("That you are liable to pay Loan amount of Rs 9696.5 (Rupees Nine Thousand Six Hundred NinetySix Paise Fifty Only) to our client so our client.", 75, customerInformationTop)

    .font("Helvetica-Bold")
    .fontSize(12)
    .text("7.", 60, customerInformationTop+35)
    .font("Helvetica")
    .text("Our client has decided to give you final chance by offering you one – time settlement of Rs {{amount}} by waiving 100% of your dues. The offer is valid till 16th Nov 2020 you can pay using Kissht app or through link which was sent to you on your phone or you may also call our representative number mentioned at end of this notice for any assistance regarding payment. Also note that repayment will also make you eligible for further loans with Kissht which is currently blocked by system due to non – payment of loan amount from your end.", 75, customerInformationTop+35)
    
    .font("Helvetica-Bold")
    .fontSize(12)
    .text("8.", 60, customerInformationTop+140)
    .font("Helvetica")
    .text("That you {{loaneeName}} have failed in making such payment despite of request & various correspondences which are on record of our client.", 75, customerInformationTop+140)
    
    .font("Helvetica-Bold")
    .fontSize(12)
    .text("9.", 60, customerInformationTop+175)
    .font("Helvetica")
    .text("Failure of you my client says, to comply with the requisitions so made in the preceding paragraphs of this notice shall render my client with no option that to take & initiate suitable legal actions entirely at your own risk cost & consequences. That looking at the above circumstances, our client may file criminal complaint against you under Sections U/S 420 for cheating and dishonestly inducing delivery of services; U/S 415 Cheating; U/S 506 Punishment for Criminal Intimidation and other Sections under Indian Penal Code, 1860 </span> in case of your failure to make payments. Accordingly, you are hereby called upon to pay to our client the amount of Rs {{amount}} for Invoice raised against you for loan amount which has been provided to you within 15 days of receipt of this notice.", 75, customerInformationTop+175)
    
    .text(`You are also called upon to pay a sum of Rs.5,000/ against the cost of this legal notice. In the event of any failure and/ or neglect to comply with the requirement of this notice, our clients shall be constrained to initiate necessary legal proceedings against you at your own risks and responsibility as to cost and consequences of it.`, 50, customerInformationTop+310)

    .text(`In case you want to make payment of your outstanding loan amount you can contact:`, 50, customerInformationTop+380)
    .text(`Name: Mohammed abdul kaleem`, 50, customerInformationTop+395)
    .text(`Contact number: 7075081556`, 50, customerInformationTop+410)
    
    .font("Helvetica-Bold")
    .fontSize(13)
    .text(`Sd/-`, 50, customerInformationTop+440)

    // .image("./test3.png", 50, 620, { width: 100 })
    // .fillColor("#444444")

    .font("Helvetica-Bold")
    .fontSize(14)
    .text(`ADV. VINAYAK GHADGE`, 50, customerInformationTop+550)
    
    .moveDown();
}


function generateFooter(doc,pageNumber) {
  doc
    .fontSize(10)
    .text(
        pageNumber,
      50,
      780,
      { align: "center", width: 500 }
    );
}

module.exports = {
  createInvoice
};