for $product in doc("products.xml")//product
    let $pid := $product/@pid
    for $item in doc("purchaseorders.xml")//PurchaseOrder/item[partid = $pid]
        let $price := $product/description/price
        let $quan := $item/quantity
        let $total := $price * $quan
group by $pid
order by $pid
return <totalcost partid = "{$pid}">{round(sum($total),2)}</totalcost>
