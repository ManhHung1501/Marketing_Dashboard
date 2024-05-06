$(function(){
    $(".related-widget-wrapper").each(function(idx) {
        // Add class if m2m widget exists
        let numSelectorChooser = $(this).find("script").length;
        console.log(numSelectorChooser)
        if (numSelectorChooser === 2) {
            let $addBtn = $(this).find("a.related-widget-wrapper-link.add-related");
            console.log($addBtn)
            $addBtn.addClass("sortedm2m-filter-horizontal-widget-add-button");

        }
    });
});